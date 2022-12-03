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

func TestFuncRpad(t *testing.T) {
	fn := proto.MustGetFunc(FuncRpad)
	assert.Equal(t, 3, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		inThird  proto.Value
		want     string
	}
	for _, v := range []tt{
		{"hi", 5, "?", "hi???"},
		{"Hello", 0, "", ""},
		{"Hello", 0, "!", ""},
		{"Hello", 3, "", ""},
		{"Hello ", 1, "World", "H"},
		{"Hello ", 2, "World", "He"},
		{"Hello ", 3, "World", "Hel"},
		{"Hello ", 4, "World", "Hell"},
		{"Hello ", 5, "World", "Hello"},
		{"Hello ", 6, "World", "Hello "},
		{"Hello ", 7, "World", "Hello W"},
		{"Hello ", 8, "World", "Hello Wo"},
		{"Hello ", 9, "World", "Hello Wor"},
		{"Hello ", 10, "World", "Hello Worl"},
		{"Hello ", 11, "World", "Hello World"},
		{"你好", 0, "", ""},
		{"你好", 0, "!", ""},
		{"你好", 3, "", ""},
		{"你好 ", 1, "世界", "你"},
		{"你好 ", 2, "世界", "你好"},
		{"你好 ", 3, "世界", "你好 "},
		{"你好 ", 4, "世界", "你好 世"},
		{"你好 ", 5, "世界", "你好 世界"},
		{"你好 ", 6, "世界", "你好 世界世"},
		{"你好 ", 7, "世界", "你好 世界世界"},
		{"你好 世界", 8, "!", "你好 世界!!!"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst), proto.ToValuer(v.inSecond), proto.ToValuer(v.inThird))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
