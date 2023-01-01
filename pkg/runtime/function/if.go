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
)

// FuncIf is  https://dev.mysql.com/doc/refman/5.6/en/flow-control-functions.html#function_if
const FuncIf = "IF"

var _ proto.Func = (*ifFunc)(nil)

func init() {
	proto.RegisterFunc(FuncIf, ifFunc{})
}

type ifFunc struct{}

func (i ifFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	val3, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	isTrue, err := val1.Int64()
	if isTrue != 0 && val2 != nil {
		return val2, nil
	} else {
		return val3, nil
	}
}

func (i ifFunc) NumInput() int {
	return 3
}
