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
func NewXAHook() (*TxLogManager, error) {
	return nil, nil
}

// xaHook XA transaction-related hook implementation
// case 1: Modify the execution action of branchTx
type xaHook struct {
	tm *TxLogManager
}

func (xh *xaHook) OnActive(tx runtime.CompositeTx) {
	tx.SetBeginFunc(StartXA)
}

func (xh *xaHook) OnPreparing(tx runtime.CompositeTx) {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetPrepareFunc(PrepareXA)
	})
}

func (xh *xaHook) OnPrepared(tx runtime.CompositeTx) {

}

func (xh *xaHook) OnCommitting(tx runtime.CompositeTx) {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetCommitFunc(CommitXA)
	})
}

func (xh *xaHook) OnCommitted(tx runtime.CompositeTx) {

}

func (xh *xaHook) OnAborting(tx runtime.CompositeTx) {
	tx.Range(func(tx runtime.BranchTx) {
		tx.SetCommitFunc(RollbackXA)
	})
}

func (xh *xaHook) OnRollbackOnly(tx runtime.CompositeTx) {

}
