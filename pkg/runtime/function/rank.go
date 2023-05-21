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
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncRank is  https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
const FuncRank = "RANK"

var _ proto.Func = (*rankFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRank, rankFunc{})
}

type rankFunc struct{}

func (a rankFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	first, err := inputs[0].Value(ctx)
	if first == nil || err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncCumeDist)
	}
	firstDec, _ := first.Float64()
	firstNum := 0

	for _, it := range inputs[1:] {
		val, err := it.Value(ctx)
		if val == nil || err != nil {
			return nil, errors.Wrapf(err, "cannot eval %s", FuncCumeDist)
		}
		valDec, _ := val.Float64()

		if valDec < firstDec {
			firstNum++
		}
	}

	return proto.NewValueInt64(int64(firstNum) + 1), nil
}

func (a rankFunc) NumInput() int {
	return 0
}
