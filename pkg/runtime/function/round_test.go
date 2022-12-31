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

func TestRound(t *testing.T) {
	fn := proto.MustGetFunc(FuncRound)
	assert.Equal(t, 2, fn.NumInput())

	type tt struct {
		desc string
		in   proto.Value
		in2  proto.Value
		out  string
	}

	for _, it := range []tt{
		{
			"ROUND(12,0)",
			proto.NewValueInt64(12),
			proto.NewValueInt64(0),
			"12",
		},

		{
			"ROUND(12.34,1)",
			proto.NewValueFloat64(12.34),
			proto.NewValueInt64(1),
			"12.3",
		},
		{
			"ROUND(-1.999,2)",
			proto.NewValueFloat64(-1.999),
			proto.NewValueInt64(2),
			"-2",
		},
		{
			"ROUND(-5.1256,2)",
			proto.MustNewValueDecimalString("-5.1256"),
			proto.NewValueInt64(2),
			"-5.13",
		},
		{
			"ROUND('foobar',-1)",
			proto.NewValueString("foobar"),
			proto.NewValueInt64(-1),
			"0",
		},
	} {
		t.Run(it.desc, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in), proto.ToValuer(it.in2))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
