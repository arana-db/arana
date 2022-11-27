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
	"strings"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/runes"
)

// FuncCastNchar is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastNchar = "CAST_NCHAR"

var _ proto.Func = (*castncharFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastNchar, castncharFunc{})
}

type castncharFunc struct{}

func (a castncharFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(inputs) < 2 {
		return val1, nil
	}
	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d2, _ := gxbig.NewDecFromString(fmt.Sprint(val2))
	if d2.IsNegative() {
		return "", errors.New("NCHAR[(N) Variable N is not allowed to be negative")
	}
	if !strings.Contains(fmt.Sprint(val2), ".") {
		num, err := d2.ToInt()
		if err != nil {
			return "", err
		}
		return getResult(runes.ConvertToRune(val1), num)
	}
	num, err := d2.ToFloat64()
	if err != nil {
		return "", err
	}
	return getResult(runes.ConvertToRune(val1), int64(num))
}

func (a castncharFunc) NumInput() int {
	return 2
}

func getResult(runes []rune, num int64) (string, error) {
	if num > int64(len(runes)) {
		return string(runes), nil
	} else if num >= 0 {
		return string(runes[:num]), nil
	}
	return "", errors.New("NCHAR[(N) Variable N is not allowed to be negative")
}
