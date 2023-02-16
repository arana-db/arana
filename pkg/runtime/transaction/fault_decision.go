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

// TxFaultDecisionExecutor Decisions of transaction pocket
// regularly scan the `__arana_tx_log` table to query the list of unfinished transactions
// case 1: If it is in the prepare state, if it exceeds a certain period of time, the transaction will be rolled back directly
// case 2: If it is in the Committing state, commit the transaction again and end the current transaction
// case 3: If it is in Aborting state, roll back the transaction again and end the current transaction
// important!!! the execution of this task requires distributed task preemption based on the metadata DB
type TxFaultDecisionExecutor struct {
	tm *TxLogManager
}

// Run Core logic of the decision -making decision -making at the bottom of the affairs
func (bm *TxFaultDecisionExecutor) Run() {

}

func (bm *TxFaultDecisionExecutor) scanUnFinishTxLog() ([]TrxLog, error) {
	return nil, nil
}

func (bm *TxFaultDecisionExecutor) handlePreparing(tx TrxLog) {

}

func (bm *TxFaultDecisionExecutor) handleCommitting(tx TrxLog) {

}

func (bm *TxFaultDecisionExecutor) handleAborting(tx TrxLog) {

}
