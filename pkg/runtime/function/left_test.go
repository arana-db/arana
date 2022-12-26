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

func TestLeft(t *testing.T) {
	fn := proto.MustGetFunc(FuncLeft)
	assert.Equal(t, 2, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		want     string
	}

	for _, it := range []tt{
		{proto.NewValueString("hello, world"), proto.NewValueInt64(2), "he"},
		{proto.NewValueString("hello, world"), proto.NewValueInt64(6), "hello,"},
		{proto.NewValueString("hello, world"), proto.NewValueInt64(20), "hello, world"},
		{proto.NewValueString("hello, world"), proto.NewValueFloat64(3.1415), "hel"},
		{proto.NewValueString("hello, world"), proto.NewValueFloat64(1.1415), "h"},
		{proto.NewValueString("hello, world"), proto.NewValueFloat64(-3.1415), ""},
		{proto.NewValueString("hello, world"), proto.NewValueInt64(0), ""},
		{proto.NewValueString("hello, world"), proto.NewValueFloat64(-0.1), ""},
		{proto.NewValueString("hello, world"), proto.NewValueString("3"), "hel"},
		{proto.NewValueString("hello, world"), proto.NewValueString("14"), "hello, world"},
		{proto.NewValueString("hello, world"), proto.NewValueString("1.1415"), "h"},
		{proto.NewValueString("hello, world"), proto.NewValueString("-3.1415"), ""},
		{proto.NewValueString("hello, world"), proto.NewValueString("test"), ""},

		{proto.NewValueString("hello, world"), nil, "NULL"},
		{nil, proto.NewValueFloat64(3.1415), "NULL"},
		{nil, nil, "NULL"},
		{nil, nil, "NULL"},

		{proto.NewValueInt64(144), proto.NewValueInt64(3), "144"},
		{proto.NewValueInt64(144), proto.NewValueInt64(2), "14"},
		{proto.NewValueFloat64(-3.14), proto.NewValueInt64(3), "-3."},
		{proto.NewValueFloat64(2.77), proto.NewValueString("2"), "2."},
		{proto.NewValueString(""), proto.NewValueString("2"), ""},
	} {
		t.Run(it.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.inFirst), proto.ToValuer(it.inSecond))
			assert.NoError(t, err)
			var actual string
			if out == nil {
				actual = "NULL"
			} else {
				actual = out.String()
			}

			assert.Equal(t, it.want, actual)
		})
	}
}
