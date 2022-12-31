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

func TestRight(t *testing.T) {
	fn := proto.MustGetFunc(FuncRight)
	assert.Equal(t, 2, fn.NumInput())

	type tt struct {
		in1 proto.Value
		in2 int64
		out string
	}

	for _, it := range []tt{
		{proto.NewValueInt64(0), 1, "0"},
		{proto.NewValueInt64(144), 1, "4"},
		{proto.NewValueFloat64(-3.14), 2, "14"},
		{proto.NewValueFloat64(2.77), 2, "77"},
		{mustDecimal("12.3"), 2, ".3"},
		{proto.NewValueString("20"), 2, "20"},
		{proto.NewValueString("11.11"), 1, "1"},
		{proto.NewValueString("foobar"), 3, "bar"},
		{proto.NewValueString("TeST"), 2, "ST"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in1), proto.ToValuer(proto.NewValueInt64(it.in2)))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
