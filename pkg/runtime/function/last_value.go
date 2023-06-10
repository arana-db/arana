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

// FuncLastValue is  https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
const FuncLastValue = "LAST_VALUE"

var _ proto.Func = (*lastvalueFunc)(nil)

func init() {
	proto.RegisterFunc(FuncLastValue, lastvalueFunc{})
}

type lastvalueFunc struct{}

func (a lastvalueFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) < 3 {
		return proto.NewValueString(""), nil
	}

	// order by this column
	firstOrderColumn, err := inputs[0].Value(ctx)
	if firstOrderColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncLastValue)
	}
	firstOrderColumnStr := firstOrderColumn.String()
	// partition by this column
	firstPartitionColumn, err := inputs[1].Value(ctx)
	if firstPartitionColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncLastValue)
	}
	firstPartitionColumnStr := firstPartitionColumn.String()
	// output by this volumn
	firstValueColumn, err := inputs[2].Value(ctx)
	if firstValueColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncLastValue)
	}
	firstValueColumnDec, _ := firstValueColumn.Float64()
	lastValue := 0.0
	startOffset := 3

	if len(inputs) < 6 {
		return proto.NewValueFloat64(firstValueColumnDec), nil
	}

	for {
		orderColumn, err := inputs[startOffset].Value(ctx)
		if orderColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncLastValue)
		}
		orderColumnStr := orderColumn.String()
		partitionColumn, err := inputs[startOffset+1].Value(ctx)
		if partitionColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncLastValue)
		}
		partitionColumnStr := partitionColumn.String()
		valueColumn, err := inputs[startOffset+2].Value(ctx)
		if valueColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncLastValue)
		}
		valueColumnDec, _ := valueColumn.Float64()
		if strings.Compare(firstOrderColumnStr, orderColumnStr) == 0 &&
			strings.Compare(firstPartitionColumnStr, partitionColumnStr) == 0 &&
			firstValueColumnDec == valueColumnDec {
			lastValue = valueColumnDec
			break
		}

		startOffset += 3
		if startOffset >= len(inputs) {
			break
		}
	}

	return proto.NewValueFloat64(lastValue), nil
}

func (a lastvalueFunc) NumInput() int {
	return 1
}
