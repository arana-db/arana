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

func TestRand(t *testing.T) {
	fn := proto.MustGetFunc(FuncRand)
	assert.Equal(t, 0, fn.NumInput())

	type tt struct {
		in  proto.Value
		out string
	}

	v, err := fn.Apply(context.Background())
	assert.NoError(t, err)
	f, err := v.Float64()
	assert.NoError(t, err)
	assert.True(t, f < 1)

	for _, it := range []tt{
		{proto.NewValueInt64(0), "0.9451961492941164"},
		{proto.NewValueInt64(1), "0.6046602879796196"},
		{proto.NewValueInt64(100), "0.8165026937796166"},
		{proto.NewValueInt64(-1), "0.3951876009960174"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
