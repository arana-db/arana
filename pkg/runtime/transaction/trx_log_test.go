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
	"encoding/json"
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
	testTrxLog := TrxLog{
		TrxID:        "test_delete_id",
		ServerID:     1,
		State:        runtime.TrxActive,
		Participants: []TrxParticipant{{NodeID: "1", RemoteAddr: "127.0.0.1", Schema: "schema"}},
		Tenant:       "test_tenant",
	}
	trxIdVal, _ := proto.NewValue("test_delete_id")
	mockDB.EXPECT().Call(
		context.Background(),
		"DELETE FROM __arana_trx_log WHERE trx_id = ?",
		gomock.Eq([]proto.Value{trxIdVal}),
	).Return(nil, uint16(0), nil).Times(1)
	err := txLogManager.DeleteTxLog(testTrxLog)
	assert.NoError(t, err)
}

func TestAddOrUpdateTxLog(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockDB := testdata.NewMockDB(ctrl)
	txLogManager := &TxLogManager{
		sysDB: mockDB,
	}
	testTrxLog := TrxLog{
		TrxID:        "test_add_or_update_id",
		ServerID:     1,
		State:        runtime.TrxActive,
		Participants: []TrxParticipant{{NodeID: "1", RemoteAddr: "127.0.0.1", Schema: "schema"}},
		Tenant:       "test_tenant",
	}
	participants, err := json.Marshal(testTrxLog.Participants)
	assert.NoError(t, err)
	trxIdVal, _ := proto.NewValue(testTrxLog.TrxID)
	tenantVal, _ := proto.NewValue(testTrxLog.Tenant)
	serverIdVal, _ := proto.NewValue(testTrxLog.ServerID)
	stateVal, _ := proto.NewValue(int32(testTrxLog.State))
	participantsVal, _ := proto.NewValue(string(participants))

	args := []proto.Value{
		trxIdVal,
		tenantVal,
		serverIdVal,
		stateVal,
		participantsVal,
	}
	mockDB.EXPECT().Call(
		context.Background(),
		"REPLACE INTO __arana_trx_log(trx_id, tenant, server_id, status, participant, start_time, update_time) VALUES (?,?,?,?,?,sysdate(),sysdate())",
		args,
	).Return(nil, uint16(0), nil).Times(1)
	err = txLogManager.AddOrUpdateTxLog(testTrxLog)
	assert.NoError(t, err)
}
