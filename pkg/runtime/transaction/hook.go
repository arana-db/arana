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

package transaction

import (
	"context"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
)

import (
	"github.com/arana-db/arana/pkg/runtime"
)

type (
	handleFunc func(ctx context.Context, tx runtime.CompositeTx) error
)

// NewXAHook creates new XAHook
func NewXAHook(tenant string, enable bool) (*xaHook, error) {
	trxMgr, err := GetTrxManager(tenant)
	if err != nil {
		return nil, err
	}

	xh := &xaHook{
		enable: enable,
	}

	trxStateChangeFunc := map[rcontext.TxState]handleFunc{
		rcontext.TrxStarted:       xh.onStarted,
		rcontext.TrxPreparing:     xh.onPreparing,
		rcontext.TrxPrepared:      xh.onPrepared,
		rcontext.TrxCommitting:    xh.onCommitting,
		rcontext.TrxCommitted:     xh.onCommitted,
		rcontext.TrxAborted:       xh.onAborting,
		rcontext.TrxRolledBacking: xh.onRollbackOnly,
		rcontext.TrxRolledBacked:  xh.onRolledBack,
	}

	xh.trxMgr = trxMgr
	xh.trxLog = &GlobalTrxLog{}
	xh.trxStateChangeFunc = trxStateChangeFunc

	return xh, nil
}

// xaHook XA transaction-related hook implementation
// case 1: Modify the execution action of branchTx
type xaHook struct {
	enable             bool
	trxMgr             *TrxManager
	trxLog             *GlobalTrxLog
	trxStateChangeFunc map[rcontext.TxState]handleFunc
}

func (xh *xaHook) OnTxStateChange(ctx context.Context, state rcontext.TxState, tx runtime.CompositeTx) error {
	if !xh.enable {
		return nil
	}
	xh.trxLog.Status = state
	handle, ok := xh.trxStateChangeFunc[state]
	if ok {
		return handle(ctx, tx)
	}
	return nil
}

// OnCreateBranchTx Fired when BranchTx create
func (xh *xaHook) OnCreateBranchTx(ctx context.Context, tx runtime.BranchTx) {
	if !xh.enable {
		return
	}
	// TODO: add branch trx log
	//xh.trxLog.BranchTrxLogs = append(xh.trxLog.BranchTrxLogs, BranchTrxLog{
	//	NodeID:     "",
	//	RemoteAddr: tx.GetConn().GetDatabaseConn().GetNetConn().RemoteAddr().String(),
	//	Schema:     tx.GetConn().DBName(),
	//})
}

func (xh *xaHook) onStarted(ctx context.Context, tx runtime.CompositeTx) error {
	tx.SetBeginFunc(StartXA)
	xh.trxLog.TrxID = tx.GetTrxID()
	xh.trxLog.Status = tx.GetTxState()
	xh.trxLog.Tenant = tx.GetTenant()
	xh.trxLog.StartTime = tx.GetStartTime()
	xh.trxLog.ExpectedEndTime = tx.GetExpectedEndTime()

	return nil
}

func (xh *xaHook) onPreparing(ctx context.Context, tx runtime.CompositeTx) error {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetPrepareFunc(PrepareXA)
	})
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	return nil
}

func (xh *xaHook) onPrepared(ctx context.Context, tx runtime.CompositeTx) error {
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	return nil
}

func (xh *xaHook) onCommitting(ctx context.Context, tx runtime.CompositeTx) error {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetCommitFunc(CommitXA)
	})
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	return nil
}

func (xh *xaHook) onCommitted(ctx context.Context, tx runtime.CompositeTx) error {
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	return nil
}

func (xh *xaHook) onAborting(ctx context.Context, tx runtime.CompositeTx) error {
	tx.Range(func(bTx runtime.BranchTx) {
		bTx.SetCommitFunc(RollbackXA)
	})
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	// auto execute XA rollback action
	tx.Range(func(bTx runtime.BranchTx) {
		bTx.Rollback(ctx)
	})
	return nil
}

func (xh *xaHook) onRollbackOnly(ctx context.Context, tx runtime.CompositeTx) error {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetCommitFunc(RollbackXA)
	})
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	return nil
}

func (xh *xaHook) onRolledBack(ctx context.Context, tx runtime.CompositeTx) error {
	xh.trxLog.Status = rcontext.TrxRolledBacking
	if err := xh.trxMgr.trxLog.AddOrUpdateGlobalTxLog(*xh.trxLog); err != nil {
		return err
	}
	return nil
}
