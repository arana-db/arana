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
	"strconv"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/math"
)

// FuncCastDecimal is https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastDecimal = "CAST_DECIMAL"

var _ proto.Func = (*castDecimalFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastDecimal, castDecimalFunc{})
}

type castDecimalFunc struct{}

func (a castDecimalFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) != 3 {
		return "", errors.New("The Decimal function must accept three parameters\n")
	}

	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	v := math.ToDecimal(val)

	maxNum, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if maxNum == nil {
		maxNum = 0
	}
	m, err := strconv.Atoi(fmt.Sprint(maxNum))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	precision, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if precision == nil {
		precision = 0
	}
	d, err := strconv.Atoi(fmt.Sprint(precision))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if m > 0 || d > 0 {
		buf, err := v.ToBin(m, d)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		var dec2 gxbig.Decimal
		_, err = dec2.FromBin(buf, m, d)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return &dec2, nil
	}
	return v, nil

}

func (a castDecimalFunc) NumInput() int {
	return 3
}
