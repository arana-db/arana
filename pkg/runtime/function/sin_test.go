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

func TestSin(t *testing.T) {
	fn := proto.MustGetFunc(FuncSin)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  interface{}
		out string
	}

	for _, it := range []tt{
		{math.Pi / 2, "1"},
		{(3 * math.Pi) / 2, "-1"},
		{1, "0.8414709848078965"},
		{0, "0"},
		{100, "-0.5063656411097588"},
	} {
		t.Run(it.out, func(t *testing.T) {
			first, _ := proto.NewValue(it.in)
			out, err := fn.Apply(context.Background(), proto.ToValuer(first))
			assert.NoError(t, err)
			var actual string
			if out == nil {
				actual = "NULL"
			} else {
				actual = out.String()
			}
			assert.Equal(t, it.out, actual)
		})
	}
}
