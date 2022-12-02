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
	"github.com/spf13/cast"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncSin https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_sign
const FuncSin = "SIN"

var _ proto.Func = (*sin)(nil)

func init() {
	proto.RegisterFunc(FuncSin, sin{})
}

type sin struct{}

// Apply call the current function.
func (s sin) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) > 1 {
		return nil, errors.New("Incorrect parameter count in the call to native function sin")
	}

	param, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return math.Sin(cast.ToFloat64(param)), nil
}

// NumInput returns the minimum number of inputs.
func (s sin) NumInput() int {
	return 1
}
