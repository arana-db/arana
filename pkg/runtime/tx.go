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

type TxState int32

const (
	_         TxState = iota
	Active            // CompositeTx 默认状态
	Preparing         // 开始执行第一条 SQL 语句
	Prepared          // 所有SQL语句均执行完，并且在 Commit 语句执行之前
	Commiting         // Prepared 完成之后，准备开始执行 Commit
	Commited          // 正式完成 Commit 动作
	Aborting          // 分支事务执行过程中出现异常，复合事务禁止继续执行
	RollbackOnly
)

// CompositeTxReadOnly 不能改边事务状态，仅仅能查询事务当前的状态信息
type CompositeTxReadOnly interface {
	GetTxState() TxState
}

// CompositeTxWriteOnly 可以推进事务状态变化以及数据变化
type CompositeTxWriteOnly interface {
}

type TxHook interface {
	OnActive(tx CompositeTxReadOnly)
	OnPreparing(tx CompositeTxReadOnly)
	OnPrepared(tx CompositeTxReadOnly)
	OnCommitting(tx CompositeTxReadOnly)
	OnCommitted(tx CompositeTxReadOnly)
	OnAborting(tx CompositeTxReadOnly)
	OnRollbackOnly(tx CompositeTxReadOnly)
}

func newCompositeTx(pi *defaultRuntime, hooks ...TxHook) *compositeTx {
	tx := &compositeTx{
		id:      gtid.NewID(),
		rt:      pi,
		txs:     make(map[string]*branchTx),
		hooks:   hooks,
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

	rt  *defaultRuntime
	txs map[string]*branchTx

	hooks []TxHook
}

func (tx *compositeTx) Version(ctx context.Context) (string, error) {
	return tx.rt.Version(ctx)
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
	newborn, err := db.(*AtomDB).begin(ctx)
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

	// save in gtid_log
	tx.setTxState(Prepared)
	return nil
}

func (tx *compositeTx) doCommit(ctx context.Context) error {
	tx.setTxState(Commiting)

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
		return err
	}

	tx.setTxState(Commited)
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

func (tx *compositeTx) GetTxState() TxState {
	return tx.txState
}

func (tx *compositeTx) setTxState(state TxState) {
	tx.txState = state

	switch state {
	case Active:
		for i := range tx.hooks {
			tx.hooks[i].OnActive(tx)
		}
	case Preparing:
		for i := range tx.hooks {
			tx.hooks[i].OnPreparing(tx)
		}
	case Prepared:
		for i := range tx.hooks {
			tx.hooks[i].OnPreparing(tx)
		}
	case Commiting:
		for i := range tx.hooks {
			tx.hooks[i].OnCommitting(tx)
		}
	case Commited:
		for i := range tx.hooks {
			tx.hooks[i].OnCommitted(tx)
		}
	case Aborting:
		for i := range tx.hooks {
			tx.hooks[i].OnAborting(tx)
		}
	case RollbackOnly:
		for i := range tx.hooks {
			tx.hooks[i].OnRollbackOnly(tx)
		}
	}
}

type prepareFunc func(ctx context.Context, bc *mysql.BackendConnection) error

type branchTx struct {
	closed atomic.Bool
	parent *AtomDB

	prepare prepareFunc
	bc      *mysql.BackendConnection
}

func (tx *branchTx) Commit(ctx context.Context) (res proto.Result, warn uint16, err error) {
	_ = ctx
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	if res, err = tx.bc.ExecuteWithWarningCount("commit", true); err != nil {
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
	return tx.prepare(ctx, tx.bc)
}

func (tx *branchTx) Rollback(ctx context.Context) (res proto.Result, warn uint16, err error) {
	if !tx.closed.CAS(false, true) {
		err = errTxClosed
		return
	}
	defer tx.dispose()
	res, err = tx.bc.ExecuteWithWarningCount("rollback", true)
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
