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

func TestLeft(t *testing.T) {
	fn := proto.MustGetFunc(FuncLeft)
	assert.Equal(t, 2, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		want     string
	}

	for _, it := range []tt{
		{"hello, world", 2, "he"},
		{"hello, world", 6, "hello,"},
		{"hello, world", 20, "hello, world"},
		{"hello, world", 3.1415, "hel"},
		{"hello, world", 1.1415, "h"},
		{"hello, world", -3.1415, ""},
		{"hello, world", 0, ""},
		{"hello, world", -0.1, ""},
		{"hello, world", "3", "hel"},
		{"hello, world", "14", "hello, world"},
		{"hello, world", "1.1415", "h"},
		{"hello, world", "-3.1415", ""},
		{"hello, world", "test", ""},

		{"hello, world", "NULL", "NULL"},
		{"NULL", 3.1415, "NULL"},
		{"NULL", "NULL", "NULL"},
		{"Null", "NULL", "NULL"},

		{int64(144), 3, "144"},
		{int64(144), 2, "14"},
		{-3.14, 3, "-3."},
		{float32(2.77), "2", "2."},
		{"", "2", ""},
	} {
		t.Run(it.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.inFirst), proto.ToValuer(it.inSecond))
			assert.NoError(t, err)
			assert.Equal(t, it.want, fmt.Sprint(out))
		})
	}
}
