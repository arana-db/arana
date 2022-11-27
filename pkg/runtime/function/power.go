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
	"math"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/shopspring/decimal"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncPower is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_power
const FuncPower = "POWER"

var _ proto.Func = (*powerFunc)(nil)

func init() {
	proto.RegisterFunc(FuncPower, powerFunc{})
}

type powerFunc struct{}

func (a powerFunc) NumInput() int {
	return 2
}

func (a powerFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	x, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	y, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	decPower := func(d *gxbig.Decimal, b *gxbig.Decimal) *gxbig.Decimal {
		tempX, _ := decimal.NewFromString(d.String())
		tempY, _ := decimal.NewFromString(b.String())
		s := tempX.Pow(tempY)
		d, _ = gxbig.NewDecFromString(s.String())
		return d
	}
	var temp float64
	switch v := y.(type) {
	case uint8:
		temp = float64(v)
	case uint16:
		temp = float64(v)
	case uint32:
		temp = float64(v)
	case uint64:
		temp = float64(v)
	case int8:
		temp = float64(v)
	case int16:
		temp = float64(v)
	case int32:
		temp = float64(v)
	case int64:
		temp = float64(v)
	case int:
		temp = float64(v)
	case float32:
		temp = float64(v)
	case float64:
		temp = v
	case *gxbig.Decimal:
		var d, b *gxbig.Decimal
		if d, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return "NaN", nil
		}
		if b, err = gxbig.NewDecFromString(fmt.Sprint(y)); err != nil {
			return "NaN", nil
		}
		return decPower(d, b), nil
	default:
		return "NaN", nil
	}
	switch v := x.(type) {
	case uint8:
		return math.Pow(float64(v), temp), nil
	case uint16:
		return math.Pow(float64(v), temp), nil
	case uint32:
		return math.Pow(float64(v), temp), nil
	case uint64:
		return math.Pow(float64(v), temp), nil
	case uint:
		return math.Pow(float64(v), temp), nil
	case int64:
		return math.Pow(float64(v), temp), nil
	case int32:
		return math.Pow(float64(v), temp), nil
	case int16:
		return math.Pow(float64(v), temp), nil
	case int8:
		return math.Pow(float64(v), temp), nil
	case int:
		return math.Pow(float64(v), temp), nil
	case float64:
		return math.Pow(v, temp), nil
	case float32:
		return math.Pow(float64(v), temp), nil
	default:
		var d, b *gxbig.Decimal
		if d, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return "NaN", nil
		}
		if b, err = gxbig.NewDecFromString(fmt.Sprint(y)); err != nil {
			return "NaN", nil
		}
		return decPower(d, b), nil
	}
}
