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

func TestStrcmp(t *testing.T) {
	fn := proto.MustGetFunc(FuncStrcmp)
	assert.Equal(t, 2, fn.NumInput())
	type tt struct {
		inFirst  interface{}
		inSecond interface{}
		want     string
	}
	for _, v := range []tt{
		{"text", "text2", "-1"},
		{"text2", "text", "1"},
		{"text", "text", "0"},
	} {
		t.Run(fmt.Sprint(v.inFirst), func(t *testing.T) {
			first, _ := proto.NewValue(v.inFirst)
			second, _ := proto.NewValue(v.inSecond)
			out, err := fn.Apply(context.Background(), proto.ToValuer(first), proto.ToValuer(second))
			assert.NoError(t, err)
			var actual string
			if out == nil {
				actual = "NULL"
			} else {
				actual = out.String()
			}
			assert.Equal(t, v.want, actual)
		})
	}
}
