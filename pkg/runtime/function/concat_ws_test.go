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

func TestConcatWSFunc_Apply(t *testing.T) {
	fn := proto.MustGetFunc(FuncConcatWS)
	assert.Equal(t, 2, fn.NumInput())

	tests := []struct {
		inputs []proto.Value
		out    string
	}{
		{
			[]proto.Value{
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
			},
			"2",
		},
		{
			[]proto.Value{
				proto.NewValueString(","),
				proto.NewValueString("open"),
				proto.NewValueString("source"),
			},
			"open,source",
		},
		{
			[]proto.Value{
				proto.NewValueString(","),
				proto.NewValueString("open"),
				nil,
				proto.NewValueString("source"),
			},
			"open,source",
		},
		{
			[]proto.Value{
				proto.NewValueString(","),
				proto.NewValueString("open"),
				proto.NewValueString("source"),
				nil,
			},
			"open,source",
		},
		{
			[]proto.Value{
				nil,
				proto.NewValueString("open"),
				proto.NewValueString("source"),
			},
			"NULL",
		},
	}

	for _, next := range tests {
		t.Run(fmt.Sprint(next.out), func(t *testing.T) {
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
