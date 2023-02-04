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

// FuncAcos https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_acos
const FuncAcos = "ACOS"

var _ proto.Func = (*acosFunc)(nil)

func init() {
	proto.RegisterFunc(FuncAcos, acosFunc{})
}

type acosFunc struct{}

// Apply call the current function.
func (s acosFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	param, err := inputs[0].Value(ctx)
	if param == nil || err != nil {
		return nil, errors.WithStack(err)
	}
	f, _ := param.Float64()
	if f < -1 || f > 1 {
		return nil, nil
	}
	return proto.NewValueFloat64(math.Acos(f)), nil
}

// NumInput returns the minimum number of inputs.
func (s acosFunc) NumInput() int {
	return 1
}
