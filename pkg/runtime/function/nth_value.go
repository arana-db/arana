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

// FuncNthValue is  https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
const FuncNthValue = "NTH_VALUE"

var _ proto.Func = (*nthvalueFunc)(nil)

func init() {
	proto.RegisterFunc(FuncNthValue, nthvalueFunc{})
}

type nthvalueFunc struct{}

func (a nthvalueFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) < 7 {
		return proto.NewValueString(""), nil
	}

	// nth
	nth, err := inputs[0].Value(ctx)
	if nth == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
	}
	nthInt, _ := nth.Int64()
	// order by this column
	firstOrderColumn, err := inputs[1].Value(ctx)
	if firstOrderColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
	}
	firstOrderColumnStr := firstOrderColumn.String()
	// partition by this column
	firstPartitionColumn, err := inputs[2].Value(ctx)
	if firstPartitionColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
	}
	firstPartitionColumnStr := firstPartitionColumn.String()
	// output by this volumn
	firstValueColumn, err := inputs[3].Value(ctx)
	if firstValueColumn == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
	}
	firstValueColumnDec, _ := firstValueColumn.Float64()
	nthIndex := int64(0)
	nthValue := 0.0
	startOffset := 4

	for {
		orderColumn, err := inputs[startOffset].Value(ctx)
		if orderColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
		}
		orderColumnStr := orderColumn.String()
		partitionColumn, err := inputs[startOffset+1].Value(ctx)
		if partitionColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
		}
		partitionColumnStr := partitionColumn.String()
		valueColumn, err := inputs[startOffset+2].Value(ctx)
		if valueColumn == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncNthValue)
		}
		valueColumnDec, _ := valueColumn.Float64()

		if strings.Compare(firstPartitionColumnStr, partitionColumnStr) == 0 {
			nthIndex += 1
			if nthIndex == nthInt {
				nthValue = valueColumnDec
				break
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

	if nthIndex < nthInt {
		return proto.NewValueString(""), nil
	} else {
		return proto.NewValueFloat64(nthValue), nil
	}
}

func (a nthvalueFunc) NumInput() int {
	return 1
}
