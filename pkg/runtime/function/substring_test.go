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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestSubstring(t *testing.T) {
	type tt struct {
		input  []proto.Value
		output string
	}

	fn := proto.MustGetFunc(FuncSubstring)

	testGroup := []tt{
		// normal
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(5)}, "ratically"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(-3)}, "ila"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(5), proto.NewValueInt64(6)}, "ratica"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(5), proto.NewValueInt64(0)}, ""},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(-5), proto.NewValueInt64(3)}, "aki"},
		// `pos` or `len` is number/not number string
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueString("5")}, "ratically"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueString("-3")}, "ila"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueString("5"), proto.NewValueString("6")}, "ratica"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueString("-5"), proto.NewValueString("3")}, "aki"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueString("e")}, ""},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueString("")}, ""},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(2), proto.NewValueString("m")}, ""},
		// has `NULL`
		{[]proto.Value{proto.NewValueString("Quadratically"), nil}, "NULL"},
		{[]proto.Value{nil, proto.NewValueInt64(5)}, "NULL"},
		{[]proto.Value{proto.NewValueString("Quadratically"), nil, proto.NewValueInt64(6)}, "NULL"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(5), nil}, "NULL"},
		{[]proto.Value{nil, proto.NewValueInt64(5), proto.NewValueInt64(6)}, "NULL"},
		{[]proto.Value{nil, nil, nil}, "NULL"},
		// has boolean
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueBool(true)}, "Quadratically"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueBool(false)}, ""},
		{[]proto.Value{proto.NewValueBool(true), proto.NewValueInt64(1)}, "1"},
		{[]proto.Value{proto.NewValueBool(false), proto.NewValueInt64(1)}, "0"},
		{[]proto.Value{proto.NewValueBool(true), proto.NewValueBool(true)}, "1"},
		{[]proto.Value{proto.NewValueBool(true), proto.NewValueBool(false)}, ""},
		{[]proto.Value{proto.NewValueBool(false), proto.NewValueBool(true)}, "0"},
		{[]proto.Value{proto.NewValueBool(false), proto.NewValueBool(false)}, ""},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(5), proto.NewValueBool(true)}, "r"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueInt64(5), proto.NewValueBool(false)}, ""},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueBool(true), proto.NewValueInt64(6)}, "Quadra"},
		{[]proto.Value{proto.NewValueString("Quadratically"), proto.NewValueBool(false), proto.NewValueInt64(6)}, ""},
		{[]proto.Value{proto.NewValueBool(true), proto.NewValueInt64(1), proto.NewValueInt64(1)}, "1"},
		{[]proto.Value{proto.NewValueBool(false), proto.NewValueInt64(1), proto.NewValueInt64(1)}, "0"},
		{[]proto.Value{proto.NewValueBool(true), proto.NewValueBool(true), proto.NewValueBool(true)}, "1"},
		{[]proto.Value{proto.NewValueBool(false), proto.NewValueBool(true), proto.NewValueBool(true)}, "0"},
		{[]proto.Value{proto.NewValueBool(false), proto.NewValueBool(false), proto.NewValueBool(false)}, ""},
		// has float
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueFloat64(4.2)}, "ila"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueFloat64(4.7)}, "la"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(1), proto.NewValueFloat64(2.1)}, "Sa"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(1), proto.NewValueFloat64(2.7)}, "Sak"},
		// pos > len(str)
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(6)}, "a"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(7)}, ""},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(99999)}, ""},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(-6)}, "Sakila"},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(-7)}, ""},
		{[]proto.Value{proto.NewValueString("Sakila"), proto.NewValueInt64(-99999)}, ""},
	}

	for _, next := range testGroup {
		var inputs []proto.Valuer
		for _, val := range next.input {
			inputs = append(inputs, proto.ToValuer(val))
		}
		out, err := fn.Apply(context.Background(), inputs...)
		assert.NoError(t, err)
		var actual string
		if out == nil {
			actual = "NULL"
		} else {
			actual = out.String()
		}
		assert.Equal(t, next.output, actual)
	}
}
