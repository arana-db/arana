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

func TestSHA1(t *testing.T) {
	fn := proto.MustGetFunc(FuncSHA1)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  proto.Value
		out string
	}

	for _, it := range []tt{
		{proto.NewValueInt64(1111), "011c945f30ce2cbafc452f39840f025693339c42"},
		{proto.NewValueString("arana"), "89d3a51a45e32f45306619da2d6cd61843f3cdb7"},
		{proto.NewValueString("db-mesh"), "cf13a5efa2393ac3ca537d4c257df5140773e752"},
		{proto.NewValueFloat64(20.22), "2a77c9db640d42dc5175f2a5bec3fa9253ae574b"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
