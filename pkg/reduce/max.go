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
	"time"
)

import (
	"github.com/shopspring/decimal"
)

var _ Reducer = (*maxReducer)(nil)

type maxReducer struct{}

func (m maxReducer) Int64(prev, next int64) (int64, error) {
	if next > prev {
		return next, nil
	}
	return prev, nil
}

func (m maxReducer) Float64(prev, next float64) (float64, error) {
	if next > prev {
		return next, nil
	}
	return prev, nil
}

func (m maxReducer) Decimal(prev, next decimal.Decimal) (decimal.Decimal, error) {
	if next.GreaterThan(prev) {
		return next, nil
	}
	return prev, nil
}

func (m maxReducer) Time(prev, next time.Time) (time.Time, error) {
	if next.After(prev) {
		return next, nil
	}
	return prev, nil
}
