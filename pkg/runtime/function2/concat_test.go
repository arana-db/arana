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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

func TestConcat(t *testing.T) {
	f := proto.MustGetFunc(FuncConcat)

	type tt struct {
		inputs []proto.Value
		out    proto.Value
	}

	for _, next := range []tt{
		{[]proto.Value{"hello ", "world"}, "hello world"},
		{[]proto.Value{1, 2, 3}, "123"},
		{[]proto.Value{"hello ", 42}, "hello 42"},
		{[]proto.Value{"hello", 1, ast.Null{}}, "NULL"},
	} {
		t.Run(fmt.Sprint(next.out), func(t *testing.T) {
			var inputs []proto.Valuer
			for i := range next.inputs {
				inputs = append(inputs, proto.ToValuer(next.inputs[i]))
			}
			out, err := f.Apply(context.Background(), inputs...)
			assert.NoError(t, err)
			assert.Equal(t, next.out, out)
		})
	}
}
