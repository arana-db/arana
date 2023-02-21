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
	"math"
	"strconv"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestAcos(t *testing.T) {
	fn := proto.MustGetFunc(FuncAcos)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  interface{}
		out interface{}
	}

	for i, it := range []tt{
		{nil, nil},
		{0, math.Pi / 2},
		{1, float64(0)},
		{-1, math.Pi},
		{2, nil},
		{-2, nil},
		{"arana", math.Pi / 2},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			first, _ := proto.NewValue(it.in)
			out, err := fn.Apply(context.Background(), proto.ToValuer(first))
			assert.NoError(t, err)
			if it.out == nil {
				assert.Nil(t, out)
				return
			}
			actual, err := out.Float64()
			assert.NoError(t, err)
			assert.Equal(t, it.out, actual)
		})
	}
}
