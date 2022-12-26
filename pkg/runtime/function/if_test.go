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

// FuncIf is  https://dev.mysql.com/doc/refman/5.6/en/flow-control-functions.html#function_if
func TestIf(t *testing.T) {
	fn := proto.MustGetFunc(FuncIf)
	assert.Equal(t, 3, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		inThird  proto.Value
		want     proto.Value
	}
	for _, v := range []tt{
		{proto.NewValueBool(1 > 2), proto.NewValueInt64(2), proto.NewValueInt64(3), proto.NewValueInt64(3)},
		{proto.NewValueBool(1 < 2), proto.NewValueString("yes"), proto.NewValueString("no"), proto.NewValueString("yes")},
		{proto.NewValueBool(false), proto.NewValueString("no"), proto.NewValueString("yes"), proto.NewValueString("yes")},
		{proto.NewValueBool(1 > 2), proto.NewValueBool(true), proto.NewValueBool(false), proto.NewValueBool(false)},
		{proto.NewValueBool(1 > 2), proto.NewValueFloat64(9.009123), proto.NewValueFloat64(8.00001), proto.NewValueFloat64(8.00001)},
		{proto.NewValueBool(1 < 2), proto.NewValueDecimal(decimal.NewFromInt(0)), proto.NewValueDecimal(decimal.NewFromInt(1)), proto.NewValueDecimal(decimal.NewFromInt(0))},
	} {
		t.Run(fmt.Sprint(v.inFirst), func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst), proto.ToValuer(v.inSecond), proto.ToValuer(v.inThird))
			assert.NoError(t, err)
			assert.Equal(t, v.want, out)
		})
	}
}
