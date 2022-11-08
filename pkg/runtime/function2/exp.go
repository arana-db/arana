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
	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncExp is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_exp
const FuncExp = "EXP"

var _ proto.Func = (*expFunc)(nil)

func init() {
	proto.RegisterFunc(FuncExp, expFunc{})
}

type expFunc struct{}

func (a expFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	decExp := func(d *gxbig.Decimal) *gxbig.Decimal {
		var ret gxbig.Decimal = *d
		temp, _ := decimal.NewFromString(ret.String())
		var s, _ = temp.ExpTaylor(18)
		print(d.String())
		d, _ = gxbig.NewDecFromString(s.String())
		return d
	}

	switch v := val.(type) {
	case *gxbig.Decimal:
		return decExp(v), nil
	case uint8:
		return math.Exp(float64(v)), nil
	case uint16:
		return math.Exp(float64(v)), nil
	case uint32:
		return math.Exp(float64(v)), nil
	case uint64:
		return math.Exp(float64(v)), nil
	case uint:
		return math.Exp(float64(v)), nil
	case int64:
		return math.Exp(float64(v)), nil
	case int32:
		return math.Exp(float64(v)), nil
	case int16:
		return math.Exp(float64(v)), nil
	case int8:
		return math.Exp(float64(v)), nil
	case int:
		return math.Exp(float64(v)), nil
	default:
		var d *gxbig.Decimal
		if d, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return _oneDecimal, nil
		}
		return decExp(d), nil
	}
}

func (a expFunc) NumInput() int {
	return 1
}
