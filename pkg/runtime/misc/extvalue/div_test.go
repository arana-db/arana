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

package extvalue

import (
	"context"
	"fmt"
	"testing"
)

import (
	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

func TestDodiv(t *testing.T) {
	type tt struct {
		p        int32 // precision
		inFirst  decimal.Decimal
		inSecond decimal.Decimal
		want     decimal.Decimal
	}

	var d Div

	for _, v := range []tt{
		{4, decimal.NewFromInt(1), decimal.NewFromInt(3), decimal.NewFromFloat(0.3333)},
		{4, decimal.NewFromInt(-1), decimal.NewFromInt(3), decimal.NewFromFloat(-0.3333)},
		{4, decimal.NewFromInt(2), decimal.NewFromInt(4), decimal.NewFromFloat(0.5000)},
		{4, decimal.NewFromInt(-2), decimal.NewFromInt(4), decimal.NewFromFloat(-0.5000)},
		{5, decimal.NewFromFloat(1.5), decimal.NewFromInt(4), decimal.NewFromFloat(0.37500)},
		{5, decimal.NewFromFloat(1.5), decimal.NewFromInt(3), decimal.NewFromFloat(0.50000)},
		{5, decimal.NewFromFloat(1.5), decimal.NewFromFloat(3.5), decimal.NewFromFloat(0.42857)},
		{8, decimal.NewFromFloat(14620), decimal.NewFromFloat(9432456), decimal.NewFromFloat(0.00154997)},
		{8, decimal.NewFromFloat(24250), decimal.NewFromFloat(9432456), decimal.NewFromFloat(0.00257091)},
		{8, decimal.NewFromFloat(0.001549967), decimal.NewFromFloat(0.002570910), decimal.NewFromFloat(0.60288653)},
	} {
		t.Run(fmt.Sprint(v.want), func(t *testing.T) {

			out, err := d.DoDiv(
				context.WithValue(context.Background(), ctxPrecisionKey, v.p),
				v.inFirst,
				v.inSecond,
			)
			assert.NoError(t, err)
			fmt.Print("want", v.want, "get", out)
			assert.Equal(t, v.want.Equal(out), true)
		})
	}
}
