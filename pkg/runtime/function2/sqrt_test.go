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

package function2

import (
	"context"
	"fmt"
	"testing"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestSqrt(t *testing.T) {
	fn := proto.MustGetFunc(FuncSqrt)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  proto.Value
		out string
	}

	mustDecimal := func(s string) *gxbig.Decimal {
		d, _ := gxbig.NewDecFromString(s)
		return d
	}

	for _, it := range []tt{
		{0, "0"},
		{int64(144), "12"},
		{-3.14, "NaN"},
		{float32(2.77), "1.6643316977093238"},
		{mustDecimal("12.3"), "3.5071355833500364"},
		{"20", "4.4721359549995794"},
		{"11.11", "3.3331666624997917"},
		{"foobar", "0.0"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
