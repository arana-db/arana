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
	"database/sql"
	"fmt"
	"time"
)

import (
	perrors "github.com/pkg/errors"

	"go.opentelemetry.io/otel/trace"

	"go.uber.org/atomic"

	"golang.org/x/sync/errgroup"
)

import (
	"github.com/arana-db/arana/pkg/metrics"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	_ "github.com/arana-db/arana/pkg/runtime/function"
	"github.com/arana-db/arana/pkg/runtime/gtid"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/dal"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/ddl"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/dml"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/utility"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	_ proto.Callable       = (*branchTx)(nil)
	_ proto.VConn          = (*compositeTx)(nil)
	_ proto.Tx             = (*compositeTx)(nil)
	_ proto.VersionSupport = (*compositeTx)(nil)
)

// TxState Transaction status
type TxState int32

const (
	_          TxState = iota
	Active             // CompositeTx Default state
	Preparing          // Start executing the first SQL statement
	Prepared           // All SQL statements are executed, and before the Commit statement executes
	Committing         // After preparing is completed, ready to start execution
	Committed          // Officially complete the Commit action
	Aborting           // There are abnormalities during the execution of the branch, and the composite transaction is prohibited to continue to execute
	RollbackOnly
)

// CompositeTx distribute transaction
type (
	// CompositeTx distribute transaction
	CompositeTx interface {
		// GetTxState get cur tx state
		GetTxState() TxState
		// SetBeginFunc sets begin func
		SetBeginFunc(f dbFunc)
		// Range range branchTx map
		Range(func(tx BranchTx))
	}

	// BranchTx each atomDB transaction
	BranchTx interface {
		// SetPrepareFunc sets prepare dbFunc
		SetPrepareFunc(f dbFunc)
		// SetCommitFunc sets commit dbFunc
		SetCommitFunc(f dbFunc)
		// SetRollbackFunc sets rollback dbFunc
		SetRollbackFunc(f dbFunc)
		// GetConn gets mysql connection
		GetConn() *mysql.BackendConnection
	}

	// TxHook transaction hook
	TxHook interface {
		// OnTxStateChange Fired when CompositeTx TrxState change
		OnTxStateChange(state TxState, tx CompositeTx)
		// OnCreateBranchTx Fired when BranchTx create
		OnCreateBranchTx(tx BranchTx)
	}

	// DeadLockDog check target CompositeTx has deadlock
	DeadLockDog interface {
		// Start run deadlock detection dog, can set how long the delay starts to execute
		Start(ctx context.Context, delay time.Duration, tx CompositeTx)
		// HasDeadLock tx deadlock is occur
		HasDeadLock() bool
		// Cancel stop run deadlock detection dog
		Cancel()
	}
)

func newCompositeTx(pi *defaultRuntime, hooks ...TxHook) *compositeTx {
	tx := &compositeTx{
		id:    gtid.NewID(),
		rt:    pi,
		txs:   make(map[string]*branchTx),
		hooks: hooks,
		beginFunc: func(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
			return bc.ExecuteWithWarningCount("begin", true)
		},
	}

	tx.setTxState(Active)
	return tx
}

type compositeTx struct {
	closed atomic.Bool
	id     gtid.ID

	beginTime time.Time
	endTime   time.Time

	isoLevel sql.IsolationLevel
	txState  TxState

	beginFunc dbFunc

	rt  *defaultRuntime
	txs map[string]*branchTx

	hooks []TxHook
}

func (tx *compositeTx) Version(ctx context.Context) (string, error) {
	return tx.rt.Version(ctx)
}

func (tx *compositeTx) SetBeginFunc(f dbFunc) {
	tx.beginFunc = f
}

func (tx *compositeTx) Query(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	return tx.call(ctx, db, query, args...)
}

func (tx *compositeTx) Exec(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
	return tx.call(ctx, db, query, args...)
}

func (tx *compositeTx) call(ctx context.Context, db string, query string, args ...proto.Value) (proto.Result, error) {
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
		return nil, perrors.WithStack(err)
	}
	return res, nil
}

func (tx *compositeTx) begin(ctx context.Context, group string) (*branchTx, error) {
	if exist, ok := tx.txs[group]; ok {
		return exist, nil
	}

	// force use writeable node
	ctx = rcontext.WithWrite(ctx)
	db := selectDB(ctx, group, tx.rt.Namespace())
	if db == nil {
		return nil, perrors.Errorf("cannot get upstream database %s", group)
	}

	// begin atom tx
	newborn, err := db.(*AtomDB).begin(ctx, tx.beginFunc)
	if err != nil {
		return nil, err
	}
	tx.txs[group] = newborn
	return newborn, nil
}

func (tx *compositeTx) String() string {
	return fmt.Sprintf("tx-%s", tx.id)
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

	args := ctx.GetArgs()
	if direct := rcontext.IsDirect(ctx.Context); direct {
		var (
			group = tx.rt.Namespace().DBGroups()[0]
			atx   *branchTx
			cctx  = rcontext.WithWrite(ctx.Context)
		)
		if atx, err = tx.begin(cctx, group); err != nil {
			return
		}
		res, warn, err = atx.Call(cctx, ctx.GetQuery(), args...)
		if err != nil {
			err = perrors.WithStack(err)
		}
		return
	}

	var (
		ru   = tx.rt.Namespace().Rule()
		plan proto.Plan
	)

	ctx.Context = rcontext.WithHints(ctx.Context, ctx.Stmt.Hints)

	var opt proto.Optimizer
	if opt, err = optimize.NewOptimizer(ru, ctx.Stmt.Hints, ctx.Stmt.StmtNode, args); err != nil {
		err = perrors.WithStack(err)
		return
	}

	if plan, err = opt.Optimize(ctx); err != nil {
		err = perrors.WithStack(err)
		return
	}

	if res, err = plan.ExecIn(ctx, tx); err != nil {
		// TODO: how to warp error packet
		err = perrors.WithStack(err)
		return
	}

	return
}

func (tx *compositeTx) ID() string {
	return tx.id.String()
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

	if err := tx.doPrepareCommit(ctx); err != nil {
		return nil, 0, err
	}
	if err := tx.doCommit(ctx); err != nil {
		return nil, 0, err
	}
	log.Debugf("commit %s success: total=%d", tx, len(tx.txs))
	return resultx.New(), 0, nil
}

func (tx *compositeTx) doPrepareCommit(ctx context.Context) error {
	tx.setTxState(Preparing)

	var g errgroup.Group
	for k, v := range tx.txs {
		k, v := k, v
		// TODO Update the prepare execution method of BranchTx
		g.Go(func() error {
			if err := v.Prepare(ctx); err != nil {
				log.Errorf("prepare %s for group %s failed: %v", tx, k, err)
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		tx.setTxState(Aborting)
		return err
	}

	// save in __arana_tx_log
	tx.setTxState(Prepared)
	return nil
}

func (tx *compositeTx) doCommit(ctx context.Context) error {
	tx.setTxState(Committing)

	var g errgroup.Group
	for k, v := range tx.txs {
		k, v := k, v
		g.Go(func() error {
			if _, _, err := v.Commit(ctx); err != nil {
				log.Errorf("commit %s for group %s failed: %v", tx, k, err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	tx.setTxState(Committed)
	return nil
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

	if err := tx.doPrepareRollback(ctx); err != nil {
		return nil, 0, err
	}
	if err := tx.doRollback(ctx); err != nil {
		return nil, 0, err
	}

	log.Debugf("rollback %s success: total=%d", tx, len(tx.txs))
	return resultx.New(), 0, nil
}

func (tx *compositeTx) doPrepareRollback(ctx context.Context) error {
	tx.setTxState(Preparing)

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
		tx.setTxState(Aborting)
		return err
	}
	tx.setTxState(Prepared)
	return nil
}

func (tx *compositeTx) doRollback(ctx context.Context) error {
	tx.setTxState(RollbackOnly)

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
		return err
	}
	return nil
}

func (tx *compositeTx) Range(f func(tx BranchTx)) {
	for k, v := range tx.txs {
		_, v := k, v
		f(v)
	}
}

func (tx *compositeTx) GetTxState() TxState {
	return tx.txState
}

func (tx *compositeTx) setTxState(state TxState) {
	tx.txState = state
	for i := range tx.hooks {
		tx.hooks[i].OnTxStateChange(state, tx)
	}
}

type dbFunc func(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error)

type branchTx struct {
	closed atomic.Bool
	parent *AtomDB

	prepare  dbFunc
	commit   dbFunc
	rollback dbFunc
	bc       *mysql.BackendConnection
}

func newBranchTx(parent *AtomDB, bc *mysql.BackendConnection) *branchTx {
	return &branchTx{
		parent: parent,
		bc:     bc,
		prepare: func(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
			return nil, nil
		},
		commit: func(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
			return bc.ExecuteWithWarningCount("commit", true)
		},
		rollback: func(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
			return bc.ExecuteWithWarningCount("rollback", true)
		},
	}
}

func (tx *branchTx) Commit(ctx context.Context) (res proto.Result, warn uint16, err error) {
	_ = ctx
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	if res, err = tx.commit(ctx, tx.bc); err != nil {
		return
	}

	var affected, lastInsertId uint64

	if affected, err = res.RowsAffected(); err != nil {
		return
	}
	if lastInsertId, err = res.LastInsertId(); err != nil {
		return
	}

	res = resultx.New(resultx.WithRowsAffected(affected), resultx.WithLastInsertID(lastInsertId))
	return
}

func (tx *branchTx) Prepare(ctx context.Context) error {
	_, err := tx.prepare(ctx, tx.bc)
	return err
}

func (tx *branchTx) Rollback(ctx context.Context) (res proto.Result, warn uint16, err error) {
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	res, err = tx.rollback(ctx, tx.bc)
	return
}

func (tx *branchTx) Call(ctx context.Context, sql string, args ...proto.Value) (res proto.Result, warn uint16, err error) {
	if len(args) > 0 {
		res, err = tx.bc.PrepareQueryArgs(sql, args)
	} else {
		res, err = tx.bc.ExecuteWithWarningCountIterRow(sql)
	}
	return
}

func (tx *branchTx) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	// TODO: choose table
	var err error
	if err = tx.bc.WriteComFieldList(table, wildcard); err != nil {
		return nil, perrors.WithStack(err)
	}
	return tx.bc.ReadColumnDefinitions()
}

func (tx *branchTx) dispose() {
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

// SetPrepareFunc set prepare dbFunc
func (tx *branchTx) SetPrepareFunc(f dbFunc) {
	tx.prepare = f
}

// SetCommitFunc set commit dbFunc
func (tx *branchTx) SetCommitFunc(f dbFunc) {
	tx.commit = f
}

// SetRollbackFunc set rollback dbFunc
func (tx *branchTx) SetRollbackFunc(f dbFunc) {
	tx.rollback = f
}

func (tx *branchTx) GetConn() *mysql.BackendConnection {
	return tx.bc
}
