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

// FuncFloor is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_floor
const FuncFloor = "FLOOR"

var _ proto.Func = (*floorFunc)(nil)

func init() {
	proto.RegisterFunc(FuncFloor, floorFunc{})
}

type floorFunc struct{}

func (f floorFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	decFloor := func(d *gxbig.Decimal) *gxbig.Decimal {
		f, err := d.ToFloat64()
		if err != nil {
			return _zeroDecimal
		}
		ret, _ := gxbig.NewDecFromFloat(math.Floor(f))
		return ret
	}

	switch v := val.(type) {
	case *gxbig.Decimal:
		return decFloor(v), nil
	case uint8, uint16, uint32, uint64, uint, int64, int32, int16, int8, int:
		return v, nil
	case float64:
		return math.Floor(v), nil
	case float32:
		return math.Floor(float64(v)), nil
	default:
		var d *gxbig.Decimal
		if d, err = gxbig.NewDecFromString(fmt.Sprint(v)); err != nil {
			return 0, nil
		}

		return decFloor(d), nil
	}
}

func (f floorFunc) NumInput() int {
	return 1
}
