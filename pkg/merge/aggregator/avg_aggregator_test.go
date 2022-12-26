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

package aggregator

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestAvgAggregator(t *testing.T) {
	params := []struct {
		nums   [][]proto.Value
		result proto.Value
		valid  bool
	}{
		{
			nums: [][]proto.Value{
				{proto.NewValueInt64(1), proto.NewValueInt64(1)},
				{proto.NewValueInt64(2), proto.NewValueInt64(2)},
				{proto.NewValueInt64(7), proto.NewValueInt64(3)},
				{proto.NewValueFloat64(1.6), proto.NewValueInt64(2)},
				{proto.NewValueInt64(10), proto.NewValueInt64(5)},
				{proto.NewValueFloat64(12.12), proto.NewValueFloat64(3.86)},
			},
			result: proto.NewValueFloat64(2),
			valid:  true,
		},
		{
			nums: [][]proto.Value{
				{proto.NewValueInt64(10), proto.NewValueInt64(2)},
			},
			result: proto.NewValueFloat64(5),
			valid:  true,
		},
		{
			nums: [][]proto.Value{
				{}, {proto.NewValueInt64(123)},
			},
			result: nil,
			valid:  false,
		},
	}

	for _, param := range params {
		addAggr := AvgAggregator{}
		for _, agg := range param.nums {
			addAggr.Aggregate(agg)
		}
		resp, err := addAggr.GetResult()
		if param.result == nil {
			assert.Nil(t, resp)
		} else {
			f1, err1 := param.result.Float64()
			f2, err2 := resp.Float64()
			assert.Nil(t, err1)
			assert.Nil(t, err2)
			assert.EqualValues(t, f1, f2)
			assert.Equal(t, err, param.valid)
		}
	}
}
