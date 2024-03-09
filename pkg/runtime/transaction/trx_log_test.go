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
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
	"github.com/arana-db/arana/testdata"
)

func TestDeleteTxLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDB := testdata.NewMockDB(ctrl)
	txLogManager := &TxLogManager{
		sysDB: mockDB,
	}
	testTrxLog := GlobalTrxLog{
		TrxID:    "test_delete_id",
		ServerID: 1,
		Status:   runtime.TrxStarted,
		Tenant:   "test_tenant",
	}
	trxIdVal, _ := proto.NewValue("test_delete_id")
	mockDB.EXPECT().Call(
		context.Background(),
		"DELETE FROM __arana_trx_log WHERE trx_id = ?",
		gomock.Eq([]proto.Value{trxIdVal}),
	).Return(nil, uint16(0), nil).Times(1)
	err := txLogManager.DeleteGlobalTxLog(testTrxLog)
	assert.NoError(t, err)
}

func TestAddOrUpdateTxLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDB := testdata.NewMockDB(ctrl)
	txLogManager := &TxLogManager{
		sysDB: mockDB,
	}
	testTrxLog := GlobalTrxLog{
		TrxID:    "test_add_or_update_id",
		ServerID: 1,
		Status:   runtime.TrxStarted,
		Tenant:   "test_tenant",
	}
	trxIdVal, _ := proto.NewValue(testTrxLog.TrxID)
	tenantVal, _ := proto.NewValue(testTrxLog.Tenant)
	serverIdVal, _ := proto.NewValue(testTrxLog.ServerID)
	stateVal, _ := proto.NewValue(int32(testTrxLog.Status))

	args := []proto.Value{
		trxIdVal,
		tenantVal,
		serverIdVal,
		stateVal,
	}
	mockDB.EXPECT().Call(
		context.Background(),
		"REPLACE INTO __arana_trx_log(trx_id, tenant, server_id, status, participant, start_time, update_time) VALUES (?,?,?,?,?,sysdate(),sysdate())",
		args,
	).Return(nil, uint16(0), nil).Times(1)
	err := txLogManager.AddOrUpdateGlobalTxLog(testTrxLog)
	assert.NoError(t, err)
}
