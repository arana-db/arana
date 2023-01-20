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
	"github.com/arana-db/arana/pkg/runtime"
)

// NewXAHook creates new XAHook
func NewXAHook(tenant string) (*xaHook, error) {
	trxMgr, err := GetTrxManager(tenant)
	if err != nil {
		return nil, err
	}

	xh := &xaHook{}

	trxStateChangeFunc := map[runtime.TxState]func(tx runtime.CompositeTx){
		runtime.Active:       xh.onActive,
		runtime.Preparing:    xh.onPreparing,
		runtime.Prepared:     xh.onPrepared,
		runtime.Committing:   xh.onCommitting,
		runtime.Committed:    xh.onCommitted,
		runtime.Aborting:     xh.onAborting,
		runtime.RollbackOnly: xh.onRollbackOnly,
	}

	xh.trxMgr = trxMgr
	xh.trxStateChangeFunc = trxStateChangeFunc

	return xh, nil
}

// xaHook XA transaction-related hook implementation
// case 1: Modify the execution action of branchTx
type xaHook struct {
	trxMgr             *TrxManager
	trxStateChangeFunc map[runtime.TxState]func(tx runtime.CompositeTx)
}

func (xh *xaHook) OnTxStateChange(state runtime.TxState, tx runtime.CompositeTx) {
	handle, ok := xh.trxStateChangeFunc[state]
	if ok {
		handle(tx)
	}
}

// OnCreateBranchTx Fired when BranchTx create
func (xh *xaHook) OnCreateBranchTx(tx runtime.BranchTx) {

}

func (xh *xaHook) onActive(tx runtime.CompositeTx) {
	tx.SetBeginFunc(StartXA)
}

func (xh *xaHook) onPreparing(tx runtime.CompositeTx) {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetPrepareFunc(PrepareXA)
	})
}

func (xh *xaHook) onPrepared(tx runtime.CompositeTx) {

}

func (xh *xaHook) onCommitting(tx runtime.CompositeTx) {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetCommitFunc(CommitXA)
	})
}

func (xh *xaHook) onCommitted(tx runtime.CompositeTx) {

}

func (xh *xaHook) onAborting(tx runtime.CompositeTx) {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetCommitFunc(RollbackXA)
	})
}

func (xh *xaHook) onRollbackOnly(tx runtime.CompositeTx) {

}
