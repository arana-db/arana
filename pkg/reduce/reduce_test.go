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

package reduce

import (
	"testing"
	"time"
)

import (
	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

func TestReduce(t *testing.T) {

	now := time.Now()

	reduces := map[Reducer][]any{
		Min(): {int64(1), float64(1), decimal.New(1, 0), now},
		Max(): {int64(2), float64(2), decimal.New(2, 0), now.Add(5 * time.Second)},
		Sum(): {int64(3), float64(3), decimal.New(3, 0), nil},
	}

	for r, v := range reduces {
		i, err := r.Int64(1, 2)
		assert.NoError(t, err)
		assert.Equal(t, i, v[0])

		float, err := r.Float64(1, 2)
		assert.NoError(t, err)
		assert.Equal(t, float, v[1])

		d, err := r.Decimal(decimal.New(1, 0), decimal.New(2, 0))
		assert.NoError(t, err)
		assert.Equal(t, d, v[2])

		times, err := r.Time(now, now.Add(5*time.Second))
		if v[3] != nil {
			assert.NoError(t, err)
			assert.Equal(t, times, v[3])
		}
	}

	for r, v := range reduces {
		i, err := r.Int64(2, 1)
		assert.NoError(t, err)
		assert.Equal(t, i, v[0])

		float, err := r.Float64(2, 1)
		assert.NoError(t, err)
		assert.Equal(t, float, v[1])

		d, err := r.Decimal(decimal.New(2, 0), decimal.New(1, 0))
		assert.NoError(t, err)
		assert.Equal(t, d, v[2])

		times, err := r.Time(now.Add(5*time.Second), now)
		if v[3] != nil {
			assert.NoError(t, err)
			assert.Equal(t, times, v[3])
		}
	}
}
