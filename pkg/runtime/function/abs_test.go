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
	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestAbs(t *testing.T) {
	fn := proto.MustGetFunc(FuncAbs)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  proto.Value
		out string
	}

	for _, it := range []tt{
		{proto.NewValueInt64(0), "0"},
		{proto.NewValueInt64(-123), "123"},
		{proto.NewValueFloat64(-3.14), "3.14"},
		{proto.NewValueFloat64(2.78), "2.78"},
		{proto.NewValueDecimal(decimal.NewFromFloat32(-5.1234)), "5.1234"},
		{proto.NewValueString("-618"), "618"},
		{proto.NewValueString("-11.11"), "11.11"},
		{proto.NewValueString("foobar"), "0"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
