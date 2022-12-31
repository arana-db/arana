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
)

import (
	"github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncMod is https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_mod
const FuncMod = "MOD"

var _ proto.Func = (*modFunc)(nil)

func init() {
	proto.RegisterFunc(FuncMod, modFunc{})
}

type modFunc struct{}

func (a modFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncMod)
	}

	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncMod)
	}

	if val1 == nil || val2 == nil {
		return nil, nil
	}
	x, err := val1.Decimal()
	if err != nil {
		x = decimal.Zero
	}
	y, err := val2.Decimal()
	if err != nil {
		return nil, nil
	}

	if y.IsZero() {
		return nil, nil
	}

	z := x.Mod(y)

	switch val1.Family() {
	case proto.ValueFamilySign, proto.ValueFamilyUnsigned:
		switch val2.Family() {
		case proto.ValueFamilySign, proto.ValueFamilyUnsigned:
			return proto.NewValueInt64(z.IntPart()), nil
		}
	}

	return proto.NewValueDecimal(z), nil
}

func (a modFunc) NumInput() int {
	return 2
}
