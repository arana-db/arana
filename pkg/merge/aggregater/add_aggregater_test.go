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

package aggregater

import (
	"testing"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/stretchr/testify/assert"
)

func TestAddAggregater(t *testing.T) {
	params := []struct {
		nums   [][]interface{}
		result *gxbig.Decimal
		valid  bool
	}{
		{
			nums: [][]interface{}{
				{1}, {2}, {3}, {1.6}, {10}, {12.12},
			},
			result: newDecFromFloat(29.72),
			valid:  true,
		},
		{
			nums: [][]interface{}{
				{0.1}, {0.2}, {10.00}, {20.10}, {},
			},
			result: newDecFromFloat(30.4),
			valid:  true,
		},
	}

	for _, param := range params {
		addAggr := AddAggregater{}
		for _, agg := range param.nums {
			addAggr.Aggregate(agg)
		}
		resp, err := addAggr.GetResult()
		f1, err1 := param.result.ToFloat64()
		f2, err2 := resp.ToFloat64()
		assert.Nil(t, err1)
		assert.Nil(t, err2)
		assert.EqualValues(t, f1, f2)
		assert.Equal(t, err, param.valid)
	}
}

func newDecFromFloat(f float64) *gxbig.Decimal {
	dec := &gxbig.Decimal{}
	dec.FromFloat64(f)
	return dec
}
