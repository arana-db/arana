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
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"

	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/gtid"
)

func Test_branchTx_CallFieldList(t *testing.T) {
	type fields struct {
		closed   atomic.Bool
		parent   *AtomDB
		state    rcontext.TxState
		prepare  dbFunc
		commit   dbFunc
		rollback dbFunc
		bc       *mysql.BackendConnection
	}
	type args struct {
		ctx      context.Context
		table    string
		wildcard string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []proto.Field
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := &branchTx{
				closed:   tt.fields.closed,
				parent:   tt.fields.parent,
				state:    tt.fields.state,
				prepare:  tt.fields.prepare,
				commit:   tt.fields.commit,
				rollback: tt.fields.rollback,
				bc:       tt.fields.bc,
			}
			got, err := tx.CallFieldList(tt.args.ctx, tt.args.table, tt.args.wildcard)
			if !tt.wantErr(t, err, fmt.Sprintf("CallFieldList(%v, %v, %v)", tt.args.ctx, tt.args.table, tt.args.wildcard)) {
				return
			}
			assert.Equalf(t, tt.want, got, "CallFieldList(%v, %v, %v)", tt.args.ctx, tt.args.table, tt.args.wildcard)
		})
	}
}

func Test_compositeTx_Rollback(t *testing.T) {
	type fields struct {
		tenant    string
		closed    atomic.Bool
		id        gtid.ID
		beginTime time.Time
		endTime   time.Time
		isoLevel  sql.IsolationLevel
		txState   rcontext.TxState
		beginFunc dbFunc
		rt        *defaultRuntime
		txs       map[string]*branchTx
		hooks     []TxHook
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    proto.Result
		want1   uint16
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := &compositeTx{
				tenant:    tt.fields.tenant,
				closed:    tt.fields.closed,
				id:        tt.fields.id,
				beginTime: tt.fields.beginTime,
				endTime:   tt.fields.endTime,
				isoLevel:  tt.fields.isoLevel,
				txState:   tt.fields.txState,
				beginFunc: tt.fields.beginFunc,
				rt:        tt.fields.rt,
				txs:       tt.fields.txs,
				hooks:     tt.fields.hooks,
			}
			got, got1, err := tx.Rollback(tt.args.ctx)
			if !tt.wantErr(t, err, fmt.Sprintf("Rollback(%v)", tt.args.ctx)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Rollback(%v)", tt.args.ctx)
			assert.Equalf(t, tt.want1, got1, "Rollback(%v)", tt.args.ctx)
		})
	}
}
