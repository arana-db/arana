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

func TestLpad(t *testing.T) {
	fn := proto.MustGetFunc(FuncLpad)
	assert.Equal(t, 3, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		inThird  proto.Value
		want     string
	}

	for _, it := range []tt{
		{proto.NewValueString("hello"), proto.NewValueInt64(10), proto.NewValueString("hahaha"), "hahahhello"},
		{proto.NewValueString("hello"), proto.NewValueInt64(10), proto.NewValueString("world"), "worldhello"},
		{proto.NewValueString("hello"), proto.NewValueInt64(3), proto.NewValueString("hahaha"), "hel"},
		{proto.NewValueString("hello"), proto.NewValueInt64(10), proto.NewValueString("ha"), "hahahhello"},
		{proto.NewValueString("hello"), proto.NewValueFloat64(3.4), proto.NewValueString("h"), "hel"},
		{proto.NewValueString("hello"), proto.NewValueInt64(0), proto.NewValueString("h"), ""},
		{proto.NewValueString("hello"), proto.NewValueInt64(-3), proto.NewValueString("h"), "NULL"},
		{proto.NewValueString(""), proto.NewValueInt64(3), proto.NewValueString("ha"), "hah"},
		{proto.NewValueString("hello"), proto.NewValueInt64(5), proto.NewValueString("world"), "hello"},
		{proto.NewValueInt64(12345), proto.NewValueInt64(30), proto.NewValueString("world"), "worldworldworldworldworld12345"},
		{proto.NewValueInt64(12345), proto.NewValueInt64(7), proto.NewValueInt64(9), "9912345"},
	} {
		t.Run(it.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.inFirst), proto.ToValuer(it.inSecond), proto.ToValuer(it.inThird))
			assert.NoError(t, err)
			assert.Equal(t, it.want, fmt.Sprint(out))
		})
	}
}
