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

func TestCastDecimal(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastDecimal)
	assert.Equal(t, 3, fn.NumInput())

	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		intThird proto.Value
		out      string
	}

	for _, it := range []tt{
		{proto.NewValueInt64(15), proto.NewValueInt64(4), proto.NewValueInt64(2), "15.00"},
		{proto.NewValueInt64(15), proto.NewValueInt64(4), proto.NewValueInt64(0), "15"},
		{proto.NewValueInt64(15), proto.NewValueInt64(4), (nil), ("15")},
		{proto.NewValueInt64(15), nil, nil, ("15")},
		{proto.NewValueInt64(15), proto.NewValueInt64(0), nil, ("15")},
		{proto.NewValueFloat64(8 / 5.0), proto.NewValueInt64(11), proto.NewValueInt64(4), "1.6000"},
		{proto.NewValueString(".885"), proto.NewValueInt64(11), proto.NewValueInt64(3), "0.885"},
		{proto.NewValueString(".885"), proto.NewValueInt64(11), proto.NewValueInt64(4), "0.8850"},
		{proto.NewValueString(".885"), proto.NewValueInt64(2), proto.NewValueInt64(1), "0.9"},
		{proto.NewValueString(".885"), proto.NewValueInt64(2), nil, "1"},
		{proto.NewValueString(".885"), proto.NewValueInt64(2), proto.NewValueInt64(0), "1"},
		{proto.NewValueString(".885"), proto.NewValueInt64(20), proto.NewValueInt64(0), "1"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.inFirst), proto.ToValuer(it.inSecond), proto.ToValuer(it.intThird))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
