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
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncRight is https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_right
const FuncRight = "RIGHT"

var _ proto.Func = (*rightFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRight, rightFunc{})
}

type rightFunc struct{}

func (c rightFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	length, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	t := length.(int)
	return fmt.Sprint(val)[len(fmt.Sprint(val))-t:], nil
}

func (c rightFunc) NumInput() int {
	return 1
}
