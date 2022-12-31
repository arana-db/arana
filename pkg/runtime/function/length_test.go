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

func TestLength(t *testing.T) {
	fn := proto.MustGetFunc(FuncLength)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  proto.Value
		out int64
	}

	for _, it := range []tt{
		{proto.NewValueInt64(1111), 4},
		{proto.NewValueString("arana"), 5},
		{proto.NewValueString("db-mesh"), 7},
		{proto.NewValueFloat64(20.22), 5},
		{proto.NewValueString("hello世界"), 11},
		{proto.NewValueString("法外狂徒张三"), 18},
		{proto.NewValueString("hello&^*(arana,世界"), 21},
		{proto.NewValueString("つのだ☆HIRO"), 16},
	} {
		t.Run(it.in.String(), func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			n, _ := out.Int64()
			assert.Equal(t, it.out, n)
		})
	}
}
