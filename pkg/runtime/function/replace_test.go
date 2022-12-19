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

func TestReplace(t *testing.T) {
	type tt struct {
		str, from, to proto.Value
		output        string
	}

	fn := proto.MustGetFunc(FuncReplace)

	testGroup := []tt{
		// normal
		{
			proto.NewValueString("arana"),
			proto.NewValueString("a"),
			proto.NewValueString("A"),
			"ArAnA",
		},
		{
			proto.NewValueString("www.mysql.com"),
			proto.NewValueString("w"),
			proto.NewValueString("Ww"),
			"WwWwWw.mysql.com",
		},
		// has Int
		{
			proto.NewValueString("123abc"),
			proto.NewValueInt64(1),
			proto.NewValueString("BB"),
			"BB23abc",
		},
		{
			proto.NewValueString("123abc"),
			proto.NewValueString("a"),
			proto.NewValueInt64(232),
			"123232bc",
		},
		{
			proto.NewValueInt64(12367),
			proto.NewValueString("7"),
			proto.NewValueString("C"),
			"1236C",
		},
		// has Float
		{
			proto.NewValueString("1.23abc"),
			proto.NewValueFloat64(1.2),
			proto.NewValueString("M"),
			"M3abc",
		},
		{
			proto.NewValueString("123abc"),
			proto.NewValueString("a"),
			proto.NewValueFloat64(2.2),
			"1232.2bc",
		},
		{
			proto.NewValueFloat64(123.67),
			proto.NewValueString("1"),
			proto.NewValueString("C"),
			"C23.67",
		},
		// has NULL
		{
			proto.NewValueString("123abc"),
			nil,
			proto.NewValueString("BB"),
			"NULL",
		},
		{
			proto.NewValueString("123abc"),
			proto.NewValueString("2"),
			nil,
			"NULL",
		},
		{
			nil,
			proto.NewValueString("2"),
			proto.NewValueString("A"),
			"NULL",
		},
		// has Bool
		{
			proto.NewValueString("www.mysql.com"),
			proto.NewValueString("www"),
			proto.NewValueBool(true),
			"1.mysql.com",
		},
		{
			proto.NewValueString("www.mysql.com"),
			proto.NewValueString("mysql"),
			proto.NewValueBool(false),
			"www.0.com",
		},
		{
			proto.NewValueBool(true),
			proto.NewValueBool(true),
			proto.NewValueBool(false),
			"0",
		},

		{
			proto.NewValueString("1polu"),
			proto.NewValueBool(true),
			proto.NewValueBool(false),
			"0polu",
		},
		{
			proto.NewValueString("8h09"),
			proto.NewValueBool(false),
			proto.NewValueString("AAA"),
			"8hAAA9",
		},
	}

	for _, next := range testGroup {
		t.Run(next.output, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(next.str), proto.ToValuer(next.from), proto.ToValuer(next.to))
			assert.NoError(t, err)
			var actual string
			if out == nil {
				actual = "NULL"
			} else {
				actual = out.String()
			}
			assert.Equal(t, next.output, actual)
		})
	}
}
