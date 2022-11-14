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
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

// FuncConcat is https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_concat
const FuncConcat = "CONCAT"

var _ proto.Func = (*concatFunc)(nil)

func init() {
	proto.RegisterFunc(FuncConcat, concatFunc{})
}

type concatFunc struct{}

// Apply implements proto.Func.
func (c concatFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	var (
		sb strings.Builder
	)
	for _, it := range inputs {
		val, err := it.Value(ctx)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// according to the doc, returns NULL if any argument is NULL.
		if _, ok := val.(ast.Null); ok {
			return ast.Null{}.String(), nil
		}
		_, _ = fmt.Fprint(&sb, val)
	}
	return sb.String(), nil
}

// NumInput implements proto.Func.
func (c concatFunc) NumInput() int {
	return 1
}
