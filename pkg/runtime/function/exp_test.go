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
	"math"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestExp(t *testing.T) {
	fn := proto.MustGetFunc(FuncExp)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		desc string
		in   proto.Value
		out  float64
	}

	for _, it := range []tt{
		{"EXP(0)", proto.NewValueInt64(0), 1},
		{"EXP(-123)", proto.NewValueInt64(-123), math.Exp(-123)},
		{"EXP(-3.14)", proto.NewValueFloat64(-3.14), math.Exp(-3.14)},
		{"EXP(2.78)", proto.NewValueFloat64(2.78), math.Exp(2.78)},
		{"EXP(-5.1234)", proto.MustNewValueDecimalString("-5.1234"), math.Exp(-5.1234)},
		{"EXP('-618')", proto.NewValueString("-618"), math.Exp(-618)},
		{"EXP('-11.11')", proto.NewValueString("-11.11"), math.Exp(-11.11)},
		{"EXP('foobar')", proto.NewValueString("foobar"), math.Exp(0)},
	} {
		t.Run(it.desc, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			actual, _ := out.Float64()
			assert.Equal(t, it.out, actual)
		})
	}
}

func TestExp_Error(t *testing.T) {
	fn := proto.MustGetFunc(FuncExp)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		desc string
		in   proto.Value
		out  float64
	}

	for _, it := range []tt{
		{"EXP(Nil)", nil, 1},
	} {
		t.Run(it.desc, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.Nil(t, err)
			assert.Nil(t, out)
		})
	}
}
