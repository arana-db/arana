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
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestMd5(t *testing.T) {
	fn := proto.MustGetFunc(FuncMd5)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  proto.Value
		out string
	}

	for _, it := range []tt{
		{1111, "b59c67bf196a4758191e42f76670ceba"},
		{&gxbig.Decimal{Value: "arana"}, "9761ad4d3bfd97b86287fc7a4a136d38"},
		{"arana", "9761ad4d3bfd97b86287fc7a4a136d38"},
		{"db-mesh", "a4e7264c356b8f0c4a1c343a7cce128f"},
		{20.22, "07fddc088bedbf68f04214c0d52b1233"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
