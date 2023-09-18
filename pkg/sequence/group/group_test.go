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

package group

import (
	"context"
	"fmt"
	"io"
	"sync"
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

const (
	tenant    = "fakeTenant"
	schema    = "employees"
	tableName = "mock_group_sequence"
)

func Test_groupSequence_Acquire(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.WithValue(context.Background(), proto.ContextKeyTenant{}, tenant)
	ctx = context.WithValue(ctx, proto.ContextKeySchema{}, schema)

	// build mock row
	mockRow := testdata.NewMockRow(ctrl)
	mockRow.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest []proto.Value) error {
		dest[0] = proto.NewValueInt64(1)
		return nil
	})

	// build mock dataset
	mockDataset := testdata.NewMockDataset(ctrl)
	mockDataset.EXPECT().Next().Return(nil, io.EOF).Times(1)
	mockDataset.EXPECT().Next().Return(mockRow, nil).Times(2)

	// build mock result
	mockRes := testdata.NewMockResult(ctrl)
	mockRes.EXPECT().RowsAffected().Return(uint64(0), nil).AnyTimes()
	mockRes.EXPECT().Dataset().Return(mockDataset, nil).AnyTimes()

	// build mock transaction
	mockTx := testdata.NewMockTx(ctrl)
	mockTx.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Eq(_initGroupSequenceTableSql)).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Eq(_initGroupSequence), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Eq(_updateNextGroup), gomock.Any(), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Eq(_selectNextGroupWithXLock), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Commit(gomock.Any()).Return(mockRes, uint16(0), nil).AnyTimes()
	mockTx.EXPECT().Rollback(gomock.Any()).Return(mockRes, uint16(0), nil).AnyTimes()

	// build mock runtime
	mockRt := runtime.NewMockRuntime(ctrl)
	mockRt.EXPECT().Begin(gomock.Any()).Return(mockTx, nil).AnyTimes()

	runtime.Register(tenant, schema, mockRt)

	ctx = context.WithValue(ctx, proto.RuntimeCtxKey{}, mockRt)

	conf := proto.SequenceConfig{
		Name:   "group",
		Type:   "group",
		Option: map[string]string{_stepKey: "100"},
	}

	validate(t, ctx, conf, 0, 0, 1)
	validate(t, ctx, conf, 100, 100, 101)
}

func validate(t *testing.T, ctx context.Context, conf proto.SequenceConfig, currentVal, currentGroupMaxVal, expectVal int64) {
	seq := &groupSequence{
		mu:                 sync.Mutex{},
		tableName:          tableName,
		currentGroupMaxVal: currentGroupMaxVal,
		currentVal:         currentVal,
	}

	err := seq.Start(ctx, conf)
	assert.NoError(t, err)

	val, err := seq.Acquire(ctx)

	assert.NoError(t, err, fmt.Sprintf("acquire err : %v", err))

	curVal := seq.CurrentVal()
	assert.Equal(t, expectVal, curVal, fmt.Sprintf("acquire val: %d, cur val: %d", val, curVal))
}
