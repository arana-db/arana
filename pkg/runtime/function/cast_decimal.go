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
	"fmt"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncCastDecimal is https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastDecimal = "CAST_DECIMAL"

var (
	_defaultDecimalPrecision proto.Value = proto.NewValueInt64(10)
	_defaultDecimalScale     proto.Value = proto.NewValueInt64(0)
)

var _ proto.Func = (*castDecimalFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastDecimal, castDecimalFunc{})
}

type castDecimalFunc struct{}

func (a castDecimalFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) != 3 {
		return nil, errors.New("The Decimal function must accept three parameters\n")
	}

	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d, err := val.Decimal()
	if err != nil {
		return proto.NewValueFloat64(0), nil
	}

	precision, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if precision == nil {
		precision = _defaultDecimalPrecision
	}

	p, err := precision.Int64()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	scale, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if scale == nil {
		scale = _defaultDecimalScale
	}
	s, err := scale.Int64()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// M must be >= D
	if p < s {
		return nil, errors.WithStack(fmt.Errorf("for float(M,D), double(M,D) or decimal(M,D), M must be >= D"))
	}

	return proto.NewValueString(d.StringFixed(int32(s))), nil
}

func (a castDecimalFunc) NumInput() int {
	return 3
}
