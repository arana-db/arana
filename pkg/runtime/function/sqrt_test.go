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

func TestSqrt(t *testing.T) {
	fn := proto.MustGetFunc(FuncSqrt)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		desc  string
		in    proto.Value
		out   float64
		valid bool
	}

	for _, it := range []tt{
		{"SQRT(0)", proto.NewValueInt64(0), math.Sqrt(0), true},
		{"SQRT(144)", proto.NewValueInt64(144), math.Sqrt(144), true},
		{"SQRT(-3.14)", proto.NewValueFloat64(-3.14), 0, false},
		{"SQRT(2.77)", proto.NewValueFloat64(2.77), math.Sqrt(2.77), true},
		{"SQRT(12.3)", proto.MustNewValueDecimalString("12.3"), math.Sqrt(12.3), true},
		{"SQRT('20')", proto.NewValueString("20"), math.Sqrt(20), true},
		{"SQRT('11.11')", proto.NewValueString("11.11"), math.Sqrt(11.11), true},
		{"SQRT('foobar')", proto.NewValueString("foobar"), math.Sqrt(0), true},
	} {
		t.Run(it.desc, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			if it.valid {
				assert.NotNil(t, out)
				f, _ := out.Float64()
				assert.Equal(t, it.out, f)
			} else {
				assert.Nil(t, out)
			}
		})
	}
}
