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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

const FuncRepeat = "REPEAT"

var _ proto.Func = (*repeatFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRepeat, repeatFunc{})
}

type repeatFunc struct{}

func (a repeatFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	var sb strings.Builder

	str, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}

	val, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, err
	}

	isNull := func(val any) bool {
		_, ok := val.(ast.Null)
		return ok
	}

	if isNull(str) || isNull(val) {
		return ast.Null{}, nil
	}

	cnt := val.(int)
	for i := 0; i < cnt; i++ {
		_, _ = fmt.Fprint(&sb, str)
	}

	return sb.String(), nil
}

func (a repeatFunc) NumInput() int {
	return 2
}
