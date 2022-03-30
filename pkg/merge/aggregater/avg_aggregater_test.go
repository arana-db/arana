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

func TestAvgAggregater(t *testing.T) {
	params := []struct {
		nums   [][]interface{}
		result *gxbig.Decimal
		valid  bool
	}{
		{
			nums: [][]interface{}{
				{1, 1}, {2, 2}, {7, 3}, {1.6, 2}, {10, 5}, {12.12, 3.86},
			},
			result: newDecFromFloat(2),
			valid:  true,
		},
		{
			nums: [][]interface{}{
				{10, 2},
			},
			result: newDecFromFloat(5),
			valid:  true,
		},
		{
			nums: [][]interface{}{
				{}, {123},
			},
			result: nil,
			valid:  false,
		},
	}

	for _, param := range params {
		addAggr := AvgAggregater{}
		for _, agg := range param.nums {
			addAggr.Aggregate(agg)
		}
		resp, err := addAggr.GetResult()
		if param.result == nil {
			assert.Nil(t, resp)
		} else {
			f1, err1 := param.result.ToFloat64()
			f2, err2 := resp.ToFloat64()
			assert.Nil(t, err1)
			assert.Nil(t, err2)
			assert.EqualValues(t, f1, f2)
			assert.Equal(t, err, param.valid)
		}
	}
}
