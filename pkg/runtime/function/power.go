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
	"math"
)

import (
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

	if x == nil || y == nil {
		return nil, nil
	}

	m, _ := x.Float64()
	n, _ := y.Float64()

	z := math.Pow(m, n)

	if math.IsInf(z, 0) {
		return nil, errors.Errorf("DOUBLE value is out of range in 'POW(%v,%v)'", m, n)
	}

	return proto.NewValueFloat64(z), nil
}
