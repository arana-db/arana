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

func TestFuncCastChar(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastChar)
	assert.Equal(t, 3, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		intThird proto.Value
		want     string
	}
	for _, v := range []tt{
		{"Hello", -1, "ASCII", "Hello"},
		{"Hello", -1, "UNICODE", "\x00H\x00e\x00l\x00l\x00o"},
		{"Hello世界", -1, "CHARACTER SET gbk", "Hello\xca\xc0\xbd\xe7"},
		{"Hello世界", -1, "CHARACTER SET gb18030", "Hello\xca\xc0\xbd\xe7"},
		{"Hello世界", 5, "CHARACTER SET latin2", "Hello"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst), proto.ToValuer(v.inSecond), proto.ToValuer(v.intThird))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
