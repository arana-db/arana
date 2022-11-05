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
		return nil, errors.WithStack(err)
	}
	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	decmod := func(d1 *gxbig.Decimal, d2 *gxbig.Decimal) *gxbig.Decimal {
		var ret gxbig.Decimal
		_ = gxbig.DecimalMod(d1, d2, &ret)
		return &ret
	}
	d1 := math.ToDecimal(val1)
	d2 := math.ToDecimal(val2)
	if !math.IsZero(d1) {
		if !math.IsZero(d2) {
			ret := decmod(d1, d2)
			return ret, nil
		} else {
			return "NULL", err
		}
	} else {
		d, err := gxbig.NewDecFromString(fmt.Sprint(0))
		return d, err
	}
}

func (a modFunc) NumInput() int {
	return 2
}
