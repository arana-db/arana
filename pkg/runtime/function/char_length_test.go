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

func TestCharLength(t *testing.T) {
	fn := proto.MustGetFunc(FuncCharLength)
	assert.Equal(t, 1, fn.NumInput())
	type tt struct {
		inFirst proto.Value
		want    int64
	}
	for _, v := range []tt{
		{proto.NewValueString("Hello世界"), 7},
		{proto.NewValueString("Hello"), 5},
		{proto.NewValueString("世界"), 2},
		{proto.NewValueString("你好世界！"), 5},
	} {
		t.Run(v.inFirst.String(), func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst))
			assert.NoError(t, err)
			actual, _ := out.Int64()
			assert.Equal(t, v.want, actual)
		})
	}
}
