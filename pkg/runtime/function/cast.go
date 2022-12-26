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

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const (
	FuncCastSign     = "CAST_SIGNED"
	FuncCastUnsigned = "CAST_UNSIGNED"
)

var _ proto.Func = (*castFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastSign, castFunc(func(d decimal.Decimal) proto.Value {
		return proto.NewValueInt64(d.IntPart())
	}))
	proto.RegisterFunc(FuncCastUnsigned, castFunc(func(d decimal.Decimal) proto.Value {
		return proto.NewValueUint64(uint64(d.IntPart()))
	}))
}

type castFunc func(decimal.Decimal) proto.Value

func (c castFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if val == nil {
		return nil, nil
	}

	in, err := val.Decimal()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return c(in), nil
}

func (c castFunc) NumInput() int {
	return 1
}
