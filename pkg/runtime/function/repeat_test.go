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

func TestRepeatFunc_Apply(t *testing.T) {
	type tt struct {
		description string
		input       []proto.Value
		output      string
	}

	fn := proto.MustGetFunc(FuncRepeat)

	tests := []tt{
		{
			"REPEAT('arana',2)",
			[]proto.Value{
				proto.NewValueString("arana"),
				proto.NewValueInt64(2),
			},
			"aranaarana",
		},
		{
			"REPEAT(NULL,2)",
			[]proto.Value{
				nil,
				proto.NewValueInt64(2),
			},
			"NULL",
		},
		{
			"REPEAT('arana',0)",
			[]proto.Value{
				proto.NewValueString("arana"),
				proto.NewValueInt64(0),
			},
			"",
		},
	}

	for _, next := range tests {
		t.Run(next.description, func(t *testing.T) {
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
		})
	}
}
