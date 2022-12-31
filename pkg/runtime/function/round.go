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

func Round(val float64, precision int) float64 {
	p := math.Pow10(precision)
	return math.Floor(val*p+0.5) / p
}

func (a roundFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	first, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	second, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if first == nil || second == nil {
		return nil, nil
	}

	x, _ := first.Float64()
	precision, _ := second.Int64()

	return proto.NewValueFloat64(Round(x, int(precision))), nil
}
