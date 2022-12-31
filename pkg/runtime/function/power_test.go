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

func TestPower(t *testing.T) {
	fn := proto.MustGetFunc(FuncPower)
	assert.Equal(t, 2, fn.NumInput())

	type tt struct {
		description string
		in          proto.Value
		in2         proto.Value
		out         string
	}

	for _, it := range []tt{
		{"POW(12,0)", proto.NewValueInt64(12), proto.NewValueInt64(0), "1"},
		{"POW(12.34,2)", proto.NewValueFloat64(12.34), proto.NewValueInt64(2), "152.2756"},
		{"POW(-5,-2)", proto.NewValueFloat64(-1.999), proto.NewValueInt64(2), "3.9960010000000006"},
		{"POW(-5,-2)", proto.MustNewValueDecimalString("-5"), proto.NewValueInt64(-2), "0.04"},
		{"POW('1','foobar')", proto.NewValueString("1"), proto.NewValueString("foobar"), "1"},
	} {
		t.Run(it.description, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in), proto.ToValuer(it.in2))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}

	// POW('foobar',-1)
	_, err := fn.Apply(context.Background(), proto.ToValuer(proto.NewValueString("foobar")), proto.ToValuer(proto.NewValueInt64(-1)))
	assert.Error(t, err, "POW('foobar',-1) should return an error")
}
