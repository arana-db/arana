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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

func TestSubstring(t *testing.T) {
	type tt struct {
		input  []proto.Value
		output string
	}

	fn := proto.MustGetFunc(FuncSubstring)

	testGroup := []tt{
		// normal
		{[]proto.Value{"Quadratically", 5}, "ratically"},
		{[]proto.Value{"Sakila", -3}, "ila"},
		{[]proto.Value{"Quadratically", 5, 6}, "ratica"},
		{[]proto.Value{"Sakila", -5, 3}, "aki"},
		// `pos` or `len` is number/not number string
		{[]proto.Value{"Quadratically", "5"}, "ratically"},
		{[]proto.Value{"Sakila", "-3"}, "ila"},
		{[]proto.Value{"Quadratically", "5", "6"}, "ratica"},
		{[]proto.Value{"Sakila", "-5", "3"}, "aki"},
		{[]proto.Value{"Quadratically", "e"}, ""},
		{[]proto.Value{"Quadratically", 2, "m"}, ""},
		// has `NULL`
		{[]proto.Value{"Quadratically", ast.Null{}}, "NULL"},
		{[]proto.Value{ast.Null{}, 5}, "NULL"},
		{[]proto.Value{"Quadratically", ast.Null{}, 6}, "NULL"},
		{[]proto.Value{"Quadratically", 5, ast.Null{}}, "NULL"},
		{[]proto.Value{ast.Null{}, 5, 6}, "NULL"},
		{[]proto.Value{ast.Null{}, ast.Null{}, ast.Null{}}, "NULL"},
		// has boolean
		{[]proto.Value{"Quadratically", true}, "Quadratically"},
		{[]proto.Value{"Quadratically", false}, ""},
		{[]proto.Value{true, 1}, "1"},
		{[]proto.Value{false, 1}, "0"},
		{[]proto.Value{true, true}, "1"},
		{[]proto.Value{true, false}, ""},
		{[]proto.Value{false, true}, "0"},
		{[]proto.Value{false, false}, ""},
		{[]proto.Value{"Quadratically", 5, true}, "r"},
		{[]proto.Value{"Quadratically", 5, false}, ""},
		{[]proto.Value{"Quadratically", true, 6}, "Quadra"},
		{[]proto.Value{"Quadratically", false, 6}, ""},
		{[]proto.Value{true, 1, 1}, "1"},
		{[]proto.Value{false, 1, 1}, "0"},
		{[]proto.Value{true, true, true}, "1"},
		{[]proto.Value{false, true, true}, "0"},
		{[]proto.Value{false, false, false}, ""},
	}

	for _, next := range testGroup {
		var inputs []proto.Valuer
		for _, val := range next.input {
			inputs = append(inputs, proto.ToValuer(val))
		}
		out, err := fn.Apply(context.Background(), inputs...)
		assert.NoError(t, err)
		assert.Equal(t, next.output, fmt.Sprint(out))
	}
}
