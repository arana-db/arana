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
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/math"
)

// FuncAbs is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_abs
const FuncAbs = "ABS"

var (
	_zeroDecimal, _ = gxbig.NewDecFromString("0.0")
	_negativeOne    = gxbig.NewDecFromInt(-1)
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

	decAbs := func(d *gxbig.Decimal) *gxbig.Decimal {
		if !d.IsNegative() {
			return d
		}

		var ret gxbig.Decimal
		_ = gxbig.DecimalMul(d, _negativeOne, &ret)
		return &ret
	}

	switch v := val.(type) {
	case *gxbig.Decimal:
		return decAbs(v), nil
	case uint8, uint16, uint32, uint64, uint:
		return v, nil
	case int64:
		return math.Abs(v), nil
	case int32:
		return math.Abs(v), nil
	case int16:
		return math.Abs(v), nil
	case int8:
		return math.Abs(v), nil
	case int:
		return math.Abs(v), nil
	case float64:
		if v < 0 {
			return -v, nil
		}
		return v, nil
	case float32:
		if v < 0 {
			return -v, nil
		}
		return v, nil
	default:
		var d *gxbig.Decimal
		if d, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return _zeroDecimal, nil
		}
		return decAbs(d), nil
	}
}

func (a absFunc) NumInput() int {
	return 1
}
