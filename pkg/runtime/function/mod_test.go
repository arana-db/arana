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

func TestMod(t *testing.T) {
	fn := proto.MustGetFunc(FuncMod)
	assert.Equal(t, 2, fn.NumInput())

	type tt struct {
		infirst  proto.Value
		insecond proto.Value
		out      string
	}

	mustDecimal := func(s string) *gxbig.Decimal {
		d, _ := gxbig.NewDecFromString(s)
		return d
	}

	for _, it := range []tt{
		{0, 1, "0"},
		{int64(-123), 3, "0"},
		{-3.14, 3, "-0.14"},
		{2.78, 2, "0.78"},
		{mustDecimal("-5.1234"), 2, "-1.1234"},
		{-618, 3, "0"},
		{-11.11, 3, "-2.11"},
		{"-11.11", 2, "-1.11"},
		{"foobar", 2, "0"},
		{1, 0, "NULL"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.infirst), proto.ToValuer(it.insecond))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
