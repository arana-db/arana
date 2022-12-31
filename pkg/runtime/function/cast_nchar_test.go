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

func TestFuncCastNchar(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastNchar)
	assert.Equal(t, 2, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		want     string
	}
	for _, v := range []tt{
		{proto.NewValueString("Hello世界"), proto.NewValueInt64(2), "He"},
		{proto.NewValueString("Hello世界"), proto.NewValueInt64(6), "Hello世"},
		{proto.NewValueString("Hello世界"), proto.NewValueInt64(7), "Hello世界"},
		{proto.NewValueString("Hello世界"), proto.NewValueInt64(0), ""},
		{proto.NewValueString("Hello世界"), proto.NewValueFloat64(0.0), ""},
		{proto.NewValueString("你好世界！"), proto.NewValueInt64(4), "你好世界"},
		{proto.NewValueInt64(1234), proto.NewValueInt64(2), "12"},
		{proto.NewValueInt64(1234), proto.NewValueInt64(3), "123"},
		{proto.NewValueInt64(1234), proto.NewValueInt64(5), "1234"},
		{proto.NewValueInt64(1234), proto.NewValueInt64(0), ""},
		{proto.NewValueInt64(1234), proto.NewValueFloat64(2.6), "12"},
		{proto.NewValueInt64(1234), proto.NewValueFloat64(2.4), "12"},
		{proto.NewValueInt64(1234), proto.NewValueInt64(2.), "12"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst), proto.ToValuer(v.inSecond))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
	type tt2 struct {
		inFirst proto.Value
		want    string
	}

	for _, v := range []tt2{
		{proto.NewValueString("Hello世界"), "Hello世界"},
		{proto.NewValueString("你好世界!"), "你好世界!"},
		{proto.NewValueInt64(1234), "1234"},
		{proto.NewValueFloat64(1.2345), "1.2345"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
