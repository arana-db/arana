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

package function

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncNtile is  https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
const FuncNtile = "NTILE"

var _ proto.Func = (*ntileFunc)(nil)

func init() {
	proto.RegisterFunc(FuncNtile, ntileFunc{})
}

type ntileFunc struct{}

func (a ntileFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) < 7 {
		return proto.NewValueString(""), nil
	}

	// bucket number
	bucketNum, err := inputs[0].Value(ctx)
	if bucketNum == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
	}
	bucketNumInt, _ := bucketNum.Int64()
	if bucketNumInt <= 0 {
		return proto.NewValueString(""), nil
	}
	// order by this column
	firstOrderColumn, err := inputs[1].Value(ctx)
	if firstOrderColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
	}
	firstOrderColumnStr := firstOrderColumn.String()
	// partition by this column
	firstPartitionColumn, err := inputs[2].Value(ctx)
	if firstPartitionColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
	}
	firstPartitionColumnStr := firstPartitionColumn.String()
	// output by this volumn
	firstValueColumn, err := inputs[3].Value(ctx)
	if firstValueColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
	}
	firstValueColumnDec, _ := firstValueColumn.Float64()
	startOffset := 4
	bucketSeq := int64(1)
	bucketIndex := int64(0)
	bucketLeft := int64(0)
	bucketDiv := int64((len(inputs)-4)/3) / bucketNumInt
	bucketMod := int64((len(inputs)-4)/3) % bucketNumInt

	for {
		orderColumn, err := inputs[startOffset].Value(ctx)
		if orderColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
		}
		orderColumnStr := orderColumn.String()
		partitionColumn, err := inputs[startOffset+1].Value(ctx)
		if partitionColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
		}
		partitionColumnStr := partitionColumn.String()
		valueColumn, err := inputs[startOffset+2].Value(ctx)
		if valueColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncNtile)
		}
		valueColumnDec, _ := valueColumn.Float64()

		bucketIndex += 1
		if bucketIndex > bucketDiv {
			if bucketIndex == bucketDiv+1 && bucketLeft < bucketMod {
				bucketLeft += 1
			} else {
				bucketIndex = int64(1)
				bucketSeq += 1
			}
		}

		if strings.Compare(firstOrderColumnStr, orderColumnStr) == 0 &&
			strings.Compare(firstPartitionColumnStr, partitionColumnStr) == 0 &&
			firstValueColumnDec == valueColumnDec {
			break
		}

		startOffset += 3
		if startOffset >= len(inputs) {
			break
		}
	}

	return proto.NewValueInt64(int64(bucketSeq)), nil
}

func (a ntileFunc) NumInput() int {
	return 1
}
