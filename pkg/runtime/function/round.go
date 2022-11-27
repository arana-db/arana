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

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncRound is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_round
const FuncRound = "ROUND"

var _ proto.Func = (*roundFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRound, roundFunc{})
}

type roundFunc struct{}

func (a roundFunc) NumInput() int {
	return 2
}

func Round(val float64, precision proto.Value) float64 {
	v, _ := precision.(int)
	p := math.Pow10(v)
	return math.Floor(val*p+0.5) / p
}

func (a roundFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	x, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	precision, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	decRound := func(d *gxbig.Decimal) *gxbig.Decimal {
		if err = d.Round(d, precision.(int), 5); err != nil {
			return nil
		}
		return d
	}

	switch v := x.(type) {
	case *gxbig.Decimal:
		return decRound(v), nil
	case uint8:
		return Round(float64(v), precision), nil
	case uint16:
		return Round(float64(v), precision), nil
	case uint32:
		return Round(float64(v), precision), nil
	case uint64:
		return Round(float64(v), precision), nil
	case uint:
		return Round(float64(v), precision), nil
	case int64:
		return Round(float64(v), precision), nil
	case int32:
		return Round(float64(v), precision), nil
	case int16:
		return Round(float64(v), precision), nil
	case int8:
		return Round(float64(v), precision), nil
	case int:
		return Round(float64(v), precision), nil
	default:
		var d *gxbig.Decimal
		if d, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return "NaN", nil
		}
		return decRound(d), nil
	}
}
