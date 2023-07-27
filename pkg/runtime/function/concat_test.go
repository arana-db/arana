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

func TestConcat(t *testing.T) {
	fn := proto.MustGetFunc(FuncConcat)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		inputs []proto.Value
		out    string
	}

	for _, next := range []tt{
		{
			[]proto.Value{
				proto.NewValueString("hello"),
				proto.NewValueString("world"),
			},
			"helloworld",
		},
		{
			[]proto.Value{
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
				proto.NewValueInt64(3),
			},
			"123",
		},
		{
			[]proto.Value{
				proto.NewValueString("hello"),
				proto.NewValueInt64(42),
			},
			"hello42",
		},
		{
			[]proto.Value{
				proto.NewValueString("hello"),
				proto.NewValueInt64(1),
				nil,
			},
			"NULL",
		},
	} {
		t.Run(next.out, func(t *testing.T) {
			var inputs []proto.Valuer
			for i := range next.inputs {
				inputs = append(inputs, proto.ToValuer(next.inputs[i]))
			}
			out, err := fn.Apply(context.Background(), inputs...)
			assert.NoError(t, err)

			var actual string
			if out == nil {
				actual = "NULL"
			} else {
				actual = out.String()
			}
			assert.Equal(t, next.out, actual)
		})
	}
}
