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

package function2

import (
	"context"
	"fmt"
	"math"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncSqrt is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_abs
const FuncSqrt = "SQRT"

var _ proto.Func = (*sqrtFunc)(nil)

func init() {
	proto.RegisterFunc(FuncSqrt, sqrtFunc{})
}

type sqrtFunc struct{}

func (a sqrtFunc) NumInput() int {
	return 1
}

func (a sqrtFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	decSqrt := func(d *gxbig.Decimal) *gxbig.Decimal {
		ret := *d
		temp := *d
		judge := 100000
		for judge > 0 {
			_ = gxbig.DecimalDiv(d, &ret, &temp, 2)
			_ = gxbig.DecimalAdd(&ret, &temp, &ret)
			_ = gxbig.DecimalDiv(&ret, _twoDecimal, &ret, 2)
			_ = ret.Round(&ret, 16, 5)
			_ = gxbig.DecimalMul(&ret, &ret, &temp)
			_ = temp.Round(&temp, 16, 5)
			_ = gxbig.DecimalSub(d, &temp, &temp)
			if temp.IsNegative() {
				_ = gxbig.DecimalMul(_negativeOne, &temp, &temp)
			}
			judge -= 1
			if temp.Compare(_maxErrorDecimal) <= 0 {
				judge = 0
			}
		}
		return &ret
	}

	switch v := val.(type) {
	case *gxbig.Decimal:
		return decSqrt(v), nil
	case uint8:
		return math.Sqrt(float64(v)), nil
	case uint16:
		return math.Sqrt(float64(v)), nil
	case uint32:
		return math.Sqrt(float64(v)), nil
	case uint64:
		return math.Sqrt(float64(v)), nil
	case uint:
		return math.Sqrt(float64(v)), nil
	case int64:
		return math.Sqrt(float64(v)), nil
	case int32:
		return math.Sqrt(float64(v)), nil
	case int16:
		return math.Sqrt(float64(v)), nil
	case int8:
		return math.Sqrt(float64(v)), nil
	case int:
		return math.Sqrt(float64(v)), nil
	default:
		var d *gxbig.Decimal
		if _, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return _zeroDecimal, nil
		}
		if d, _ = gxbig.NewDecFromString(fmt.Sprint(v)); d.IsNegative() {
			return "NaN", nil
		}
		return decSqrt(d), nil
	}
}
