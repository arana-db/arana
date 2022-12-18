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
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncAbs is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_abs
const FuncAbs = "ABS"

var (
	_zeroDecimal, _     = gxbig.NewDecFromString("0.0")
	_negativeOne        = gxbig.NewDecFromInt(-1)
	_maxErrorDecimal, _ = gxbig.NewDecFromString("0.0000000000000001")
	_twoDecimal, _      = gxbig.NewDecFromString("2.0")
	_oneDecimal, _      = gxbig.NewDecFromString("1.0")
)

var _ proto.Func = (*absFunc)(nil)

func init() {
	proto.RegisterFunc(FuncAbs, absFunc{})
}

type absFunc struct{}

func (a absFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	d, err := val.Decimal()
	if err != nil {
		return proto.NewValueFloat64(0), nil
	}
	d = d.Abs()

	switch val.Family() {
	case proto.ValueFamilyFloat:
		return proto.NewValueFloat64(d.InexactFloat64()), nil
	case proto.ValueFamilyDecimal, proto.ValueFamilyString:
		return proto.NewValueDecimal(d), nil
	case proto.ValueFamilySign:
		return proto.NewValueInt64(d.IntPart()), nil
	default:
		panic("unreachable")
	}
}

func (a absFunc) NumInput() int {
	return 1
}
