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
	"strings"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

// FuncConcatWS https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_concat-ws
const FuncConcatWS = "CONCAT_WS"

var _ proto.Func = (*concatWSFunc)(nil)

func init() {
	proto.RegisterFunc(FuncConcatWS, concatWSFunc{})
}

type concatWSFunc struct{}

func (c concatWSFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	var (
		sb  strings.Builder
		sep proto.Value
		err error
	)

	if sep, err = inputs[0].Value(ctx); err != nil {
		return nil, err
	}
	// check separator is NULL
	if isNull(sep) {
		return ast.Null{}.String(), nil
	}

	for i := range inputs {
		val, err := inputs[i].Value(ctx)
		if err != nil {
			return nil, err
		}

		if i == 0 || isNull(val) {
			continue
		}

		if i != 1 {
			_, _ = fmt.Fprint(&sb, sep)
		}
		_, _ = fmt.Fprint(&sb, val)
	}

	return sb.String(), nil
}

func (c concatWSFunc) NumInput() int {
	return 2
}

func isNull(val any) bool {
	_, ok := val.(ast.Null)
	return ok
}
