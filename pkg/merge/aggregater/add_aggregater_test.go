//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package aggregater_test

import (
	"testing"
)

import (
	"github.com/dubbogo/arana/pkg/merge/aggregater"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

func TestAddAggregater(t *testing.T) {
	params := []struct {
		nums   [][]interface{}
		result decimal.Decimal
		valid  bool
	}{
		{
			nums: [][]interface{}{
				{1}, {2}, {3}, {1.6}, {10}, {12.12},
			},
			result: decimal.NewFromFloat(29.72),
			valid:  true,
		},
		{
			nums: [][]interface{}{
				{0.1}, {0.2}, {10.00}, {20.10}, {},
			},
			result: decimal.NewFromFloat(30.4),
			valid:  true,
		},
	}

	for _, param := range params {
		addAggr := aggregater.AddAggregater{}
		for _, agg := range param.nums {
			addAggr.Aggregate(agg)
		}
		resp, err := addAggr.GetResult()
		assert.EqualValues(t, param.result.BigFloat(), resp.BigFloat())
		assert.Equal(t, err, param.valid)
	}
}
