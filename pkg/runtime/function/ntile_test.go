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
)

func TestFuncNtile(t *testing.T) {
	fn := proto.MustGetFunc(FuncNtile)
	type tt struct {
		inputs [][]proto.Value
		want   string
	}
	for _, v := range []tt{
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"1",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"1",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"1",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"1",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"1",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"2",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"2",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"2",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueInt64(2)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
				{proto.NewValueString("12:00:00"), proto.NewValueString(""), proto.NewValueFloat64(100)},
				{proto.NewValueString("13:00:00"), proto.NewValueString(""), proto.NewValueFloat64(125)},
				{proto.NewValueString("14:00:00"), proto.NewValueString(""), proto.NewValueFloat64(132)},
				{proto.NewValueString("15:00:00"), proto.NewValueString(""), proto.NewValueFloat64(145)},
				{proto.NewValueString("16:00:00"), proto.NewValueString(""), proto.NewValueFloat64(140)},
				{proto.NewValueString("17:00:00"), proto.NewValueString(""), proto.NewValueFloat64(150)},
				{proto.NewValueString("18:00:00"), proto.NewValueString(""), proto.NewValueFloat64(200)},
				{proto.NewValueString("19:00:00"), proto.NewValueString(""), proto.NewValueFloat64(220)},
				{proto.NewValueString("20:00:00"), proto.NewValueString(""), proto.NewValueFloat64(260)},
			},
			"2",
		},
	} {
		t.Run(v.want, func(t *testing.T) {
			var inputs []proto.Valuer
			for i := range v.inputs {
				for j := range v.inputs[i] {
					inputs = append(inputs, proto.ToValuer(v.inputs[i][j]))
				}
			}
			out, err := fn.Apply(context.Background(), inputs...)
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
