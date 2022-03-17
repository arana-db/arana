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
	stdErrors "errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

import (
	"github.com/bwmarrin/snowflake"

	"github.com/pkg/errors"

	"go.uber.org/atomic"

	"golang.org/x/sync/errgroup"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/pkg/util/rand2"
	"github.com/arana-db/arana/third_party/pools"
)

var (
	_ Runtime     = (*defaultRuntime)(nil)
	_ proto.VConn = (*defaultRuntime)(nil)
	_ proto.VConn = (*compositeTx)(nil)
)

var (
	errTxClosed = stdErrors.New("transaction is closed")
)

func NewAtomDB(node *config.Node) *AtomDB {
	if node == nil {
		return nil
	}
	r, w, err := node.GetReadAndWriteWeight()
	if err != nil {
		return nil
	}
	db := &AtomDB{
		id:     node.Name,
		weight: proto.Weight{R: int32(r), W: int32(w)},
	}

	raw, _ := json.Marshal(map[string]interface{}{
		"dsn": fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", node.Username, node.Password, node.Host, node.Port, node.Database),
	})
	connector, _ := mysql.NewConnector(raw)
	db.pool = pools.NewResourcePool(connector.NewBackendConnection, 8, 16, 30*time.Minute, 1, nil)

	return db
}

// Runtime executes a sql statement.
type Runtime interface {
	proto.Executable
	// Namespace returns the namespace.
	Namespace() *namespace.Namespace
	// Begin begins a new transaction.
	Begin(ctx *proto.Context) (proto.Tx, error)
}

// Load loads a Runtime, here schema means logical database name.
func Load(schema string) (Runtime, error) {
	var ns *namespace.Namespace
	if ns = namespace.Load(schema); ns == nil {
		return nil, errors.Errorf("no such logical database %s", schema)
	}
	return &defaultRuntime{ns: ns}, nil
}

var (
	_ proto.DB       = (*AtomDB)(nil)
	_ proto.Callable = (*atomTx)(nil)
	_ proto.Tx       = (*compositeTx)(nil)
)

type compositeTx struct {
	closed atomic.Bool
	id     int64

	rt  *defaultRuntime
	txs map[string]*atomTx
}

func (tx *compositeTx) Query(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	if len(db) < 1 {
		db = tx.rt.Namespace().DBGroups()[0]
	}

	atx, err := tx.begin(ctx, db)
	if err != nil {
		return nil, err
	}
	res, _, err := atx.Call(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
}

func (tx *compositeTx) Exec(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (tx *compositeTx) begin(ctx context.Context, group string) (*atomTx, error) {
	if exist, ok := tx.txs[group]; ok {
		return exist, nil
	}
	newborn, err := tx.rt.Namespace().DB(ctx, group).(*AtomDB).begin(ctx)
	if err != nil {
		return nil, err
	}
	tx.txs[group] = newborn
	return newborn, nil
}

func (tx *compositeTx) String() string {
	return fmt.Sprintf("tx-%d", tx.id)
}

func (tx *compositeTx) Execute(ctx *proto.Context) (res proto.Result, warn uint16, err error) {
	if tx.closed.Load() {
		err = errTxClosed
		return
	}

	var (
		args = tx.rt.extractArgs(ctx)
	)
	if direct := rcontext.IsDirect(ctx.Context); direct {
		var (
			group = tx.rt.Namespace().DBGroups()[0]
			atx   *atomTx
			cctx  = rcontext.WithMaster(ctx.Context)
		)
		if atx, err = tx.begin(cctx, group); err != nil {
			return
		}
		res, warn, err = atx.Call(cctx, ctx.GetQuery(), args...)
		return
	}

	var (
		ru   = tx.rt.ns.Rule()
		plan proto.Plan
		c    = ctx.Context
	)

	c = rcontext.WithMaster(c)
	c = rcontext.WithRule(c, ru)
	c = rcontext.WithSQL(c, ctx.GetQuery())

	if plan, err = tx.rt.ns.Optimizer().Optimize(c, ctx.Stmt.StmtNode, args...); err != nil {
		err = errors.WithStack(err)
		return
	}

	if res, err = plan.ExecIn(c, tx); err != nil {
		// TODO: how to warp error packet
		err = errors.WithStack(err)
		return
	}

	return
}

func (tx *compositeTx) ID() int64 {
	return tx.id
}

func (tx *compositeTx) Commit(ctx context.Context) (proto.Result, uint16, error) {
	if !tx.closed.CAS(false, true) {
		return nil, 0, errTxClosed
	}

	defer func() { // cleanup
		tx.rt = nil
		tx.txs = nil
	}()

	var g errgroup.Group
	for k, v := range tx.txs {
		k := k
		v := v
		g.Go(func() error {
			_, _, err := v.Commit(ctx)
			if err != nil {
				log.Errorf("commit %s for group %s failed: %v", tx, k, err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, 0, err
	}

	log.Debugf("commit %s success: total=%d", tx, len(tx.txs))

	return &mysql.Result{}, 0, nil
}

func (tx *compositeTx) Rollback(ctx context.Context) (proto.Result, uint16, error) {
	if !tx.closed.CAS(false, true) {
		return nil, 0, errTxClosed
	}

	defer func() { // cleanup
		tx.rt = nil
		tx.txs = nil
	}()

	var g errgroup.Group
	for k, v := range tx.txs {
		k := k
		v := v
		g.Go(func() error {
			_, _, err := v.Rollback(ctx)
			if err != nil {
				log.Errorf("rollback %s for group %s failed: %v", tx, k, err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, 0, err
	}

	log.Debugf("rollback %s success: total=%d", tx, len(tx.txs))

	return &mysql.Result{}, 0, nil
}

type atomTx struct {
	closed atomic.Bool
	parent *AtomDB
	bc     *mysql.BackendConnection
}

func (tx *atomTx) Commit(ctx context.Context) (res proto.Result, warn uint16, err error) {
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	res, warn, err = tx.bc.ExecuteWithWarningCount("commit", true)
	return
}

func (tx *atomTx) Rollback(ctx context.Context) (res proto.Result, warn uint16, err error) {
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	res, warn, err = tx.bc.ExecuteWithWarningCount("rollback", true)
	return
}

func (tx *atomTx) Call(ctx context.Context, sql string, args ...interface{}) (res proto.Result, warn uint16, err error) {
	if len(args) > 0 {
		res, warn, err = tx.bc.PrepareQueryArgs(sql, args)
	} else {
		res, warn, err = tx.bc.ExecuteWithWarningCount(sql, true)
	}
	return
}

func (tx *atomTx) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	// TODO: choose table
	var err error
	if err = tx.bc.WriteComFieldList(table, wildcard); err != nil {
		return nil, errors.WithStack(err)
	}
	return tx.bc.ReadColumnDefinitions()
}

func (tx *atomTx) dispose() {
	defer func() {
		tx.parent = nil
		tx.bc = nil
	}()

	cnt := tx.parent.pendingRequests.Dec()
	tx.parent.returnConnection(tx.bc)
	if cnt == 0 && tx.parent.closed.Load() {
		tx.parent.pool.Close()
	}
}

type AtomDB struct {
	id string

	weight proto.Weight
	pool   *pools.ResourcePool

	closed atomic.Bool

	pendingRequests atomic.Int64
}

func (db *AtomDB) begin(ctx context.Context) (*atomTx, error) {
	if db.closed.Load() {
		return nil, errors.Errorf("the db instance '%s' is closed already", db.id)
	}

	var (
		bc  *mysql.BackendConnection
		err error
	)

	if bc, err = db.borrowConnection(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	db.pendingRequests.Inc()

	if _, _, err = bc.ExecuteWithWarningCount("begin", true); err != nil {
		// cleanup if failed to begin tx
		cnt := db.pendingRequests.Dec()
		db.returnConnection(bc)
		if cnt == 0 && db.closed.Load() {
			db.pool.Close()
		}
		return nil, err
	}

	return &atomTx{parent: db, bc: bc}, nil
}

func (db *AtomDB) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	if db.closed.Load() {
		return nil, errors.Errorf("the db instance '%s' is closed already", db.id)
	}

	var (
		bc  *mysql.BackendConnection
		err error
	)

	if bc, err = db.borrowConnection(ctx); err != nil {
		return nil, errors.WithStack(err)
	}

	defer db.returnConnection(bc)
	defer db.pending()

	if err = bc.WriteComFieldList(table, wildcard); err != nil {
		return nil, errors.WithStack(err)
	}

	return bc.ReadColumnDefinitions()
}

func (db *AtomDB) Call(ctx context.Context, sql string, args ...interface{}) (res proto.Result, warn uint16, err error) {
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

func (db *AtomDB) Close() error {
	if db.closed.CAS(false, true) {
		if db.pendingRequests.Load() == 0 {
			db.pool.Close()
		}
	}
	return nil
}

func (db *AtomDB) pending() func() {
	db.pendingRequests.Inc()
	return func() {
		// close pool if atom db is marked as closed, and no requests.
		if cnt := db.pendingRequests.Dec(); cnt == 0 && db.closed.Load() {
			db.pool.Close()
		}
	}
}

func (db *AtomDB) ID() string {
	return db.id
}

func (db *AtomDB) IdleTimeout() time.Duration {
	return db.pool.IdleTimeout()
}

func (db *AtomDB) MaxCapacity() int {
	return int(db.pool.MaxCap())
}

func (db *AtomDB) Capacity() int {
	return int(db.pool.Capacity())
}

func (db *AtomDB) Weight() proto.Weight {
	return db.weight
}

func (db *AtomDB) SetCapacity(capacity int) error {
	return db.pool.SetCapacity(capacity)
}

func (db *AtomDB) SetMaxCapacity(maxCapacity int) error {
	// TODO: how to set max capacity?
	return nil
}

func (db *AtomDB) SetIdleTimeout(idleTimeout time.Duration) error {
	db.pool.SetIdleTimeout(idleTimeout)
	return nil
}

func (db *AtomDB) SetWeight(weight proto.Weight) error {
	db.weight = weight
	return nil
}

func (db *AtomDB) borrowConnection(ctx context.Context) (*mysql.BackendConnection, error) {
	res, err := db.pool.Get(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return res.(*mysql.BackendConnection), nil
}

func (db *AtomDB) returnConnection(bc *mysql.BackendConnection) {
	db.pool.Put(bc)
}

type defaultRuntime struct {
	ns *namespace.Namespace
}

func (pi *defaultRuntime) Begin(ctx *proto.Context) (proto.Tx, error) {
	tx := &compositeTx{
		id:  nextTxID(),
		rt:  pi,
		txs: make(map[string]*atomTx),
	}
	log.Debugf("begin transaction: %s", tx.String())
	return tx, nil
}

func (pi *defaultRuntime) Namespace() *namespace.Namespace {
	return pi.ns
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
	args := pi.extractArgs(ctx)

	if direct := rcontext.IsDirect(ctx.Context); direct {
		return pi.ns.DB0(ctx.Context).Call(rcontext.WithMaster(ctx.Context), ctx.GetQuery(), args...)
	}

	var (
		ru   = pi.ns.Rule()
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

var (
	_txIds     *snowflake.Node
	_txIdsOnce sync.Once
)

func nextTxID() int64 {
	_txIdsOnce.Do(func() {
		_txIds, _ = snowflake.NewNode(rand2.Int63n(1024))
	})
	return _txIds.Generate().Int64()
}
