/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package runtime

import (
	"context"
	"encoding/json"
	stdErrors "errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

import (
	"github.com/bwmarrin/snowflake"

	"github.com/pkg/errors"

	"go.uber.org/atomic"

	"golang.org/x/sync/errgroup"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/metrics"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
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

	Tracer = otel.Tracer("runtime")
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
	connector, err := mysql.NewConnector(raw)
	if err != nil {
		panic(err)
	}

	var (
		capacity    = config.GetConnPropCapacity(node.ConnProps, 8)
		maxCapacity = config.GetConnPropMaxCapacity(node.ConnProps, 64)
		idleTime    = config.GetConnPropIdleTime(node.ConnProps, 30*time.Minute)
	)

	db.pool = pools.NewResourcePool(connector.NewBackendConnection, capacity, maxCapacity, idleTime, 1, nil)

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
	return &defaultRuntime{
		ns: ns,
	}, nil
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
	return tx.call(ctx, db, query, args...)
}

func (tx *compositeTx) Exec(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	return tx.call(ctx, db, query, args...)
}

func (tx *compositeTx) call(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	if len(db) < 1 {
		db = tx.rt.Namespace().DBGroups()[0]
	}

	atx, err := tx.begin(ctx, db)
	if err != nil {
		return nil, err
	}

	log.Debugf("call upstream: db=%s, sql=\"%s\", args=%v", db, query, args)

	res, _, err := atx.Call(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
}

func (tx *compositeTx) begin(ctx context.Context, group string) (*atomTx, error) {
	if exist, ok := tx.txs[group]; ok {
		return exist, nil
	}

	// force use writeable node
	ctx = rcontext.WithWrite(ctx)

	// begin atom tx
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
	var span trace.Span
	ctx.Context, span = Tracer.Start(ctx.Context, "compositeTx.Execute")
	execStart := time.Now()
	defer func() {
		span.End()
		metrics.ExecuteDuration.Observe(time.Since(execStart).Seconds())
	}()
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
			cctx  = rcontext.WithWrite(ctx.Context)
		)
		if atx, err = tx.begin(cctx, group); err != nil {
			return
		}
		res, warn, err = atx.Call(cctx, ctx.GetQuery(), args...)
		if err != nil {
			err = errors.WithStack(err)
		}
		return
	}

	var (
		ru   = tx.rt.ns.Rule()
		plan proto.Plan
		c    = ctx.Context
	)

	c = rcontext.WithRule(c, ru)
	c = rcontext.WithSQL(c, ctx.GetQuery())

	if plan, err = tx.rt.ns.Optimizer().Optimize(c, tx, ctx.Stmt.StmtNode, args...); err != nil {
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
	ctx, span := Tracer.Start(ctx, "compositeTx.Commit")
	defer func() { // cleanup
		tx.rt = nil
		tx.txs = nil
		span.End()
	}()

	var g errgroup.Group
	for k, v := range tx.txs {
		k, v := k, v
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

	return resultx.New(), 0, nil
}

func (tx *compositeTx) Rollback(ctx context.Context) (proto.Result, uint16, error) {
	ctx, span := Tracer.Start(ctx, "compositeTx.Rollback")
	defer span.End()
	if !tx.closed.CAS(false, true) {
		return nil, 0, errTxClosed
	}

	defer func() { // cleanup
		tx.rt = nil
		tx.txs = nil
	}()

	var g errgroup.Group
	for k, v := range tx.txs {
		k, v := k, v
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

	return resultx.New(), 0, nil
}

type atomTx struct {
	closed atomic.Bool
	parent *AtomDB
	bc     *mysql.BackendConnection
}

func (tx *atomTx) Commit(ctx context.Context) (res proto.Result, warn uint16, err error) {
	_ = ctx
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	if res, err = tx.bc.ExecuteWithWarningCount("commit", true); err != nil {
		return
	}

	var (
		affected, lastInsertId uint64
	)

	if affected, err = res.RowsAffected(); err != nil {
		return
	}
	if lastInsertId, err = res.LastInsertId(); err != nil {
		return
	}

	res = resultx.New(resultx.WithRowsAffected(affected), resultx.WithLastInsertID(lastInsertId))
	return
}

func (tx *atomTx) Rollback(ctx context.Context) (res proto.Result, warn uint16, err error) {
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	res, err = tx.bc.ExecuteWithWarningCount("rollback", true)
	return
}

func (tx *atomTx) Call(ctx context.Context, sql string, args ...interface{}) (res proto.Result, warn uint16, err error) {
	if len(args) > 0 {
		res, err = tx.bc.PrepareQueryArgs(sql, args)
	} else {
		res, err = tx.bc.ExecuteWithWarningCountIterRow(sql)
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

	dispose := func() {
		// cleanup if failed to begin tx
		cnt := db.pendingRequests.Dec()
		db.returnConnection(bc)
		if cnt == 0 && db.closed.Load() {
			db.pool.Close()
		}
	}

	var res proto.Result
	if res, err = bc.ExecuteWithWarningCount("begin", true); err != nil {
		defer dispose()
		return nil, errors.WithStack(err)
	}

	// NOTICE: must consume the result
	if _, err = res.RowsAffected(); err != nil {
		defer dispose()
		return nil, errors.WithStack(err)
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
	defer db.pending()()

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

	undoPending := db.pending()

	if len(args) > 0 {
		res, err = bc.PrepareQueryArgs(sql, args)
	} else {
		res, err = bc.ExecuteWithWarningCountIterRow(sql)
	}

	if err != nil {
		undoPending()
		db.returnConnection(bc)
		return
	}

	res.(*mysql.RawResult).SetCloser(func() error {
		undoPending()
		db.returnConnection(bc)
		return nil
	})

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
	bcp := (*BackendResourcePool)(db.pool)
	//var (
	//	active0, available0 = db.pool.Active(), db.pool.Available()
	//)
	res, err := bcp.Get(ctx)
	//log.Infof("^^^^^ borrow conn: %d/%d => %d/%d", available0, active0, db.pool.Active(), db.pool.Available())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
}

func (db *AtomDB) returnConnection(bc *mysql.BackendConnection) {
	db.pool.Put(bc)
	//log.Infof("^^^^^ return conn: active=%d, available=%d", db.pool.Active(), db.pool.Available())
}

type defaultRuntime struct {
	ns *namespace.Namespace
}

func (pi *defaultRuntime) Begin(ctx *proto.Context) (proto.Tx, error) {
	var span trace.Span
	ctx.Context, span = Tracer.Start(ctx, "defaultRuntime.Begin")
	defer span.End()
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
	ctx = rcontext.WithRead(ctx)
	return pi.call(ctx, db, query, args...)
}

func (pi *defaultRuntime) Exec(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	ctx = rcontext.WithWrite(ctx)
	res, err := pi.call(ctx, db, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if closer, ok := res.(io.Closer); ok {
		defer func() {
			_ = closer.Close()
		}()
	}
	return res, nil
}

func (pi *defaultRuntime) Execute(ctx *proto.Context) (res proto.Result, warn uint16, err error) {
	var span trace.Span
	ctx.Context, span = Tracer.Start(ctx.Context, "defaultRuntime.Execute")
	execStart := time.Now()
	defer func() {
		span.End()
		metrics.ExecuteDuration.Observe(time.Since(execStart).Seconds())
	}()
	args := pi.extractArgs(ctx)

	if direct := rcontext.IsDirect(ctx.Context); direct {
		return pi.callDirect(ctx, args)
	}

	var (
		ru   = pi.ns.Rule()
		plan proto.Plan
		c    = ctx.Context
	)

	c = rcontext.WithRule(c, ru)
	c = rcontext.WithSQL(c, ctx.GetQuery())
	c = rcontext.WithSchema(c, ctx.Schema)
	c = rcontext.WithDBGroup(c, pi.ns.DBGroups()[0])
	c = rcontext.WithTenant(c, ctx.Tenant)

	start := time.Now()
	if plan, err = pi.ns.Optimizer().Optimize(c, pi, ctx.Stmt.StmtNode, args...); err != nil {
		err = errors.WithStack(err)
		return
	}
	metrics.OptimizeDuration.Observe(time.Since(start).Seconds())

	if res, err = plan.ExecIn(c, pi); err != nil {
		// TODO: how to warp error packet
		err = errors.WithStack(err)
		return
	}

	return
}

func (pi *defaultRuntime) callDirect(ctx *proto.Context, args []interface{}) (res proto.Result, warn uint16, err error) {
	res, warn, err = pi.ns.DB0(ctx.Context).Call(rcontext.WithWrite(ctx.Context), ctx.GetQuery(), args...)
	if err != nil {
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

func (pi *defaultRuntime) call(ctx context.Context, group, query string, args ...interface{}) (proto.Result, error) {
	if len(group) < 1 { // empty db, select first
		if groups := pi.ns.DBGroups(); len(groups) > 0 {
			group = groups[0]
		}
	}

	db := pi.ns.DB(ctx, group)
	if db == nil {
		return nil, errors.Errorf("cannot get upstream database %s", group)
	}

	log.Debugf("call upstream: db=%s, sql=\"%s\", args=%v", group, query, args)
	// TODO: how to pass warn???
	res, _, err := db.Call(ctx, query, args...)

	return res, err
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
