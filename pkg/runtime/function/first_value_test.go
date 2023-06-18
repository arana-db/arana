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

func TestFuncFirstValue(t *testing.T) {
	fn := proto.MustGetFunc(FuncFirstValue)
	type tt struct {
		inputs [][]proto.Value
		want   string
	}
	for _, v := range []tt{
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"10",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"10",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"10",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"10",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"0",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"0",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"0",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"0",
		},
		{
			[][]proto.Value{
				//order column, partition column, value column
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("st113"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("st113"), proto.NewValueFloat64(9)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("st113"), proto.NewValueFloat64(25)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("st113"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(0)},
				{proto.NewValueString("07:15:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(10)},
				{proto.NewValueString("07:30:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(5)},
				{proto.NewValueString("07:45:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(30)},
				{proto.NewValueString("08:00:00"), proto.NewValueString("xh458"), proto.NewValueFloat64(25)},
			},
			"0",
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
