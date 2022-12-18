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

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncTruncate is https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_truncate
const FuncTruncate = "TRUNCATE"

var _ proto.Func = (*truncateFunc)(nil)

func init() {
	proto.RegisterFunc(FuncTruncate, truncateFunc{})
}

type truncateFunc struct{}

func (t truncateFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	x, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	v, err := x.Float64()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	e, err := d.Int64()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	res := math.Trunc(v*math.Pow10(int(e))) * math.Pow10(-int(e))
	return proto.NewValueDecimal(decimal.NewFromFloat(res)), nil
}

func (t truncateFunc) NumInput() int {
	return 2
}
