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

package snowflake

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

func Test_snowflakeSequence_Acquire(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// build mock row
	mockRow := testdata.NewMockRow(ctrl)
	mockRow.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest []proto.Value) error {
		dest[0] = proto.NewValueInt64(1024)
		return nil
	}).AnyTimes()

	// build mock dataset
	mockDataset := testdata.NewMockDataset(ctrl)
	mockDataset.EXPECT().Next().Return(mockRow, nil).Times(2)

	// build mock result
	mockRes := testdata.NewMockResult(ctrl)
	mockRes.EXPECT().RowsAffected().Return(uint64(0), nil).AnyTimes()
	mockRes.EXPECT().Dataset().Return(mockDataset, nil).AnyTimes()

	// build mock transaction
	mockTx := testdata.NewMockTx(ctrl)
	mockTx.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Eq(_initTableSql)).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Eq(_keepaliveNode), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Eq(_setWorkId), gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Eq(_selectSelfWorkIdWithXLock), gomock.Any(), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Eq(_selectMaxWorkIdWithXLock), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Eq(_selectFreeWorkIdWithXLock), gomock.Any()).Return(mockRes, nil).AnyTimes()
	mockTx.EXPECT().Commit(gomock.Any()).Return(mockRes, uint16(0), nil).AnyTimes()
	mockTx.EXPECT().Rollback(gomock.Any()).Return(mockRes, uint16(0), nil).AnyTimes()

	// build mock runtime
	mockRt := runtime.NewMockRuntime(ctrl)
	mockRt.EXPECT().Begin(gomock.Any()).Return(mockTx, nil).AnyTimes()

	ctx := context.WithValue(context.Background(), proto.RuntimeCtxKey{}, mockRt)

	seq := &snowflakeSequence{
		mu: sync.Mutex{},
	}

	conf := proto.SequenceConfig{
		Name: "sf-ut",
	}
	err := seq.Start(ctx, conf)
	assert.NoError(t, err)

	finishInitTable = false
	mockDataset.EXPECT().Next().Return(nil, io.EOF).Times(1)
	mockDataset.EXPECT().Next().Return(mockRow, nil).AnyTimes()
	err = seq.Start(ctx, conf)
	expectErr := fmt.Errorf("node worker-id must in [0, %d]", workIdMax)
	assert.Equal(t, expectErr, err)

	val, err := seq.Acquire(context.Background())
	assert.NoError(t, err, fmt.Sprintf("acquire err : %v", err))

	curVal := seq.CurrentVal()
	assert.Equal(t, val, curVal, fmt.Sprintf("acquire val: %d, cur val: %d", val, curVal))

	bucket := []int64{0, 0}

	total := int64(100000)
	for i := int64(0); i < total; i++ {
		ret, _ := seq.Acquire(context.Background())

		if ret%2 == 0 {
			bucket[0] = bucket[0] + 1
		} else {
			bucket[1] = bucket[1] + 1
		}
	}

	t.Logf("odd number : %0.3f", float64(bucket[1]*1.0)/float64(total))
	t.Logf("even number : %0.3f", float64(bucket[0]*1.0)/float64(total))
}
