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
	"errors"
	"fmt"
)

import (
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
)

var (
	ErrorInvalidTxId = errors.New("invalid transaction id")
)

// StartXA do start xa transaction action
func StartXA(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
	txId := rcontext.TransactionID(ctx)
	if len(txId) == 0 {
		return nil, ErrorInvalidTxId
	}

	return bc.ExecuteWithWarningCount(fmt.Sprintf("XA START '%s'", txId), false)
}

// EndXA do end xa transaction action
func EndXA(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
	txId := rcontext.TransactionID(ctx)
	if len(txId) == 0 {
		return nil, ErrorInvalidTxId
	}

	return bc.ExecuteWithWarningCount(fmt.Sprintf("XA END '%s'", txId), false)
}

// PrepareXA do prepare xa transaction action
func PrepareXA(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
	txId := rcontext.TransactionID(ctx)
	if len(txId) == 0 {
		return nil, ErrorInvalidTxId
	}

	return bc.ExecuteWithWarningCount(fmt.Sprintf("XA PREPARE '%s'", txId), false)
}

// CommitXA do commit xa transaction action
func CommitXA(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
	txId := rcontext.TransactionID(ctx)
	if len(txId) == 0 {
		return nil, ErrorInvalidTxId
	}

	return bc.ExecuteWithWarningCount(fmt.Sprintf("XA COMMIT '%s'", txId), false)
}

// RollbackXA do rollback xa transaction action
func RollbackXA(ctx context.Context, bc *mysql.BackendConnection) (proto.Result, error) {
	txId := rcontext.TransactionID(ctx)
	if len(txId) == 0 {
		return nil, ErrorInvalidTxId
	}

	return bc.ExecuteWithWarningCount(fmt.Sprintf("XA ROLLBACK '%s'", txId), false)
}
