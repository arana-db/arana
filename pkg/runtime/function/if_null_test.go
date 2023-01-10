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

// FuncIFNULL is https://dev.mysql.com/doc/refman/5.6/en/flow-control-functions.html#function_ifnull
func TestIfNull(t *testing.T) {
	fn := proto.MustGetFunc(FuncIfNull)
	assert.Equal(t, 2, fn.NumInput())
	type tt struct {
		inFirst  proto.Value
		inSecond proto.Value
		want     proto.Value
	}
	for _, v := range []tt{
		{nil, proto.NewValueInt64(2), proto.NewValueInt64(2)},
		{nil, proto.NewValueString("yes"), proto.NewValueString("yes")},
		{nil, proto.NewValueBool(true), proto.NewValueBool(true)},
		{nil, proto.NewValueFloat64(9.009123), proto.NewValueFloat64(9.009123)},
		{nil, proto.NewValueDecimal(decimal.NewFromInt(8080)), proto.NewValueDecimal(decimal.NewFromInt(8080))},
		{proto.NewValueInt64(2), proto.NewValueFloat64(9.009123), proto.NewValueInt64(2)},
		{proto.NewValueString("yes"), proto.NewValueInt64(2), proto.NewValueString("yes")},
		{proto.NewValueBool(true), proto.NewValueFloat64(9.009123), proto.NewValueBool(true)},
		{proto.NewValueFloat64(9.009123), proto.NewValueBool(true), proto.NewValueFloat64(9.009123)},
		{proto.NewValueDecimal(decimal.NewFromInt(8080)), proto.NewValueFloat64(9.009123), proto.NewValueDecimal(decimal.NewFromInt(8080))},
	} {
		t.Run(fmt.Sprint(v.inFirst), func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst), proto.ToValuer(v.inSecond))
			assert.NoError(t, err)
			assert.Equal(t, v.want, out)
		})
	}
}
