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

	d2, _ := val2.Decimal()
	s := a.getResult(runes.ConvertToRune(val1), d2.IntPart())
	return proto.NewValueString(s), nil
}

func (a castncharFunc) NumInput() int {
	return 2
}

func (a castncharFunc) getResult(runes []rune, num int64) string {
	if num > int64(len(runes)) {
		return string(runes)
	}
	return string(runes[:num])
}
