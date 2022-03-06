// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"
)

import (
	"github.com/pkg/errors"

	"go.uber.org/atomic"
)

import (
	"github.com/dubbogo/arana/pkg/mysql"
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/proto/rule"
	rcontext "github.com/dubbogo/arana/pkg/runtime/context"
	"github.com/dubbogo/arana/pkg/runtime/namespace"
	"github.com/dubbogo/arana/pkg/runtime/optimize"
	"github.com/dubbogo/arana/third_party/pools"
)

var (
	_ Runtime     = (*defaultRuntime)(nil)
	_ proto.VConn = (*defaultRuntime)(nil)
)

const (
	fakeDatabase = "employees"
	fakeGroup    = "employee1"
	fakeDsn      = "root:123456@tcp(arana-mysql:3306)/employees?timeout=11s&readTimeout=11s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
)

func init() {
	// TODO:
	// 1. how to initialize? using real configuration.
	// 2. watch config modifications, then build and enqueue namespace command.

	// FIXME: just fake init, put a fake namespace, remove it after feature completed.
	db := &myDB{
		id:     "fake-arana-mysql-1",
		weight: proto.Weight{R: 10, W: 10},
	}

	raw, _ := json.Marshal(map[string]interface{}{
		"dsn": fakeDsn,
	})
	connector, _ := mysql.NewConnector(raw)
	db.pool = pools.NewResourcePool(connector.NewBackendConnection, 8, 16, 30*time.Minute, 1, nil)

	ns := namespace.New(
		fakeDatabase,
		optimize.GetOptimizer(),
		namespace.UpdateRule(fakeRule()),
		namespace.UpsertDB(fakeGroup, db),
	)

	_ = namespace.Register(ns)
}

// Runtime executes a sql statement.
type Runtime interface {
	// Execute executes the sql context.
	Execute(ctx *proto.Context) (result proto.Result, warn uint16, err error)
}

// Load loads a Runtime, here schema means logical database name.
func Load(schema string) (Runtime, error) {
	var ns *namespace.Namespace
	if ns = namespace.Load(schema); ns == nil {
		return nil, errors.Errorf("no such logical database %s", schema)
	}
	return &defaultRuntime{ns: ns}, nil
}

type myDB struct {
	id string

	weight proto.Weight
	pool   *pools.ResourcePool

	closed atomic.Bool

	pendingRequests atomic.Int64
}

func (db *myDB) Call(ctx context.Context, sql string, args ...interface{}) (res proto.Result, warn uint16, err error) {
	if db.closed.Load() {
		err = errors.Errorf("the db instance '%s' is closed already", db.id)
		return
	}

	var bc *mysql.BackendConnection

	if bc, err = db.borrowConnection(ctx); err != nil {
		err = errors.WithStack(err)
		return
	}

	defer db.returnConnection(bc)
	defer db.pending()

	if len(args) > 0 {
		res, warn, err = bc.PrepareQueryArgs(sql, args)
	} else {
		res, warn, err = bc.ExecuteWithWarningCount(sql, true)
	}
	return
}

func (db *myDB) Close() error {
	if db.closed.CAS(false, true) {
		// TODO: graceful shutdown, check pending requests.
		db.pool.Close()
	}
	return nil
}

func (db *myDB) pending() func() {
	db.pendingRequests.Inc()
	return func() {
		_ = db.pendingRequests.Dec()
	}
}

func (db *myDB) ID() string {
	return db.id
}

func (db *myDB) IdleTimeout() time.Duration {
	return db.pool.IdleTimeout()
}

func (db *myDB) MaxCapacity() int {
	return int(db.pool.MaxCap())
}

func (db *myDB) Capacity() int {
	return int(db.pool.Capacity())
}

func (db *myDB) Weight() proto.Weight {
	return db.weight
}

func (db *myDB) SetCapacity(capacity int) error {
	return db.pool.SetCapacity(capacity)
}

func (db *myDB) SetMaxCapacity(maxCapacity int) error {
	// TODO: how to set max capacity?
	return nil
}

func (db *myDB) SetIdleTimeout(idleTimeout time.Duration) error {
	db.pool.SetIdleTimeout(idleTimeout)
	return nil
}

func (db *myDB) SetWeight(weight proto.Weight) error {
	db.weight = weight
	return nil
}

func (db *myDB) borrowConnection(ctx context.Context) (*mysql.BackendConnection, error) {
	res, err := db.pool.Get(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return res.(*mysql.BackendConnection), nil
}

func (db *myDB) returnConnection(bc *mysql.BackendConnection) {
	db.pool.Put(bc)
}

type defaultRuntime struct {
	ns *namespace.Namespace
}

func (pi *defaultRuntime) Query(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	if len(db) < 1 { // empty db, select first
		if groups := pi.ns.DBGroups(); len(groups) > 0 {
			db = groups[0]
		}
	}

	upstream := pi.ns.DB(ctx, db)
	if upstream == nil {
		return nil, errors.Errorf("cannot get upstream database %s", db)
	}

	// TODO: how to pass warn???
	res, _, err := upstream.Call(ctx, query, args...)
	return res, err
}

func (pi *defaultRuntime) Exec(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (pi *defaultRuntime) Execute(ctx *proto.Context) (res proto.Result, warn uint16, err error) {
	var (
		ru   = pi.ns.Rule()
		args = pi.extractArgs(ctx)
		plan proto.Plan
		c    = ctx.Context
	)

	c = rcontext.WithRule(c, ru)
	c = rcontext.WithSQL(c, ctx.GetQuery())

	if plan, err = pi.ns.Optimizer().Optimize(c, ctx.Stmt.StmtNode, args...); err != nil {
		err = errors.WithStack(err)
		return
	}

	if res, err = plan.ExecIn(c, pi); err != nil {
		// TODO: how to warp error packet
		err = errors.WithStack(err)
		return
	}

	return
}

func (pi *defaultRuntime) extractArgs(ctx *proto.Context) []interface{} {
	if ctx.Stmt == nil || len(ctx.Stmt.BindVars) < 1 {
		return nil
	}

	var (
		keys = make([]string, 0, len(ctx.Stmt.BindVars))
		args = make([]interface{}, 0, len(ctx.Stmt.BindVars))
	)

	for k := range ctx.Stmt.BindVars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, ctx.Stmt.BindVars[k])
	}
	return args
}

// fakeRule returns a fake rule.
// TODO: remove it after using real config.
func fakeRule() *rule.Rule {
	// student -> [student_0000..student_0031]
	var (
		ru rule.Rule
		vt rule.VTable
		tp rule.Topology
	)
	var tbls []int
	for i := 0; i < 32; i++ {
		tbls = append(tbls, i)
	}
	tp.SetTopology(0, tbls...)
	tp.SetRender(func(i int) string {
		return fakeGroup
	}, func(i int) string {
		return fmt.Sprintf("student_%04d", i)
	})
	vt.SetTopology(&tp)

	sm := &rule.ShardMetadata{
		Stepper: rule.Stepper{
			N: 1,
			U: rule.Unum,
		},
		Computer: rule.DirectShardComputer(func(i interface{}) (int, error) {
			n, err := strconv.Atoi(fmt.Sprintf("%v", i))
			if err != nil {
				return 0, err
			}
			return n % 32, nil
		}),
	}

	vt.SetShardMetadata("uid", nil, sm)
	ru.SetVTable("student", &vt)

	return &ru
}
