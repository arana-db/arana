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

func TestCastSign(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastSign)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		desc string
		in   proto.Value
		out  int64
	}

	for _, it := range []tt{
		{"CAST(0 AS SIGNED)", proto.NewValueInt64(0), 0},
		{"CAST(1 AS SIGNED)", proto.NewValueInt64(1), 1},
		{"CAST(-1 AS SIGNED)", proto.NewValueInt64(-1), -1},
	} {
		t.Run(it.desc, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			t.Logf("res: %v\n", out.String())
			assert.NoError(t, err)
			v, _ := out.Int64()
			assert.Equal(t, it.out, v)
		})
	}
}

func TestCastUnsigned(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastUnsigned)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		desc string
		in   proto.Value
		out  uint64
	}

	for _, it := range []tt{
		{
			"CAST(0 AS UNSIGNED)",
			proto.NewValueInt64(0),
			0,
		},
		{
			"CAST(1 AS UNSIGNED)",
			proto.NewValueInt64(1),
			1,
		},
		{
			"CAST(-1 AS UNSIGNED)",
			proto.NewValueInt64(-1),
			func() uint64 {
				var n int64 = -1
				return uint64(n)
			}(),
		},
	} {
		t.Run(it.desc, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			t.Logf("res: %v\n", out)
			assert.NoError(t, err)

			u, _ := out.Uint64()
			assert.Equal(t, it.out, u)
		})
	}
}
