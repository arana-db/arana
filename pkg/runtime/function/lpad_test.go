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
		{"hello", 10, "hahaha", "hahahhello"},
		{"hello", 10, "world", "worldhello"},
		{"hello", 3, "hahaha", "hel"},
		{"hello", 10, "ha", "hahahhello"},
		{"hello", 3.4, "h", "hel"},
		{"hello", 0, "h", ""},
		{"hello", -3, "h", "NULL"},
		{"", 3, "ha", "hah"},
		{"hello", 5, "world", "hello"},
		{12345, 30, "world", "worldworldworldworldworld12345"},
		{12345, 7, 9, "9912345"},
	} {
		t.Run(it.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.inFirst), proto.ToValuer(it.inSecond), proto.ToValuer(it.inThird))
			assert.NoError(t, err)
			assert.Equal(t, it.want, fmt.Sprint(out))
		})
	}
}
