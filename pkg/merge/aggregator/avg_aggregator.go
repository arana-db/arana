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
	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type AvgAggregator struct {
	sum   decimal.NullDecimal
	count decimal.NullDecimal
}

func (s *AvgAggregator) Aggregate(values []proto.Value) {
	if len(values) < 2 {
		return
	}

	if values[0] != nil {
		val1, err := values[0].Decimal()
		if err != nil {
			panic(err.Error())
		}

		if !s.sum.Valid {
			s.sum.Valid = true
			s.sum.Decimal = decimal.Zero
		}
		s.sum.Decimal = s.sum.Decimal.Add(val1)
	}

	if values[1] != nil {
		val, err := values[1].Decimal()
		if err != nil {
			panic(err.Error())
		}

		if !s.count.Valid {
			s.count.Valid = true
			s.count.Decimal = decimal.Zero
		}
		s.count.Decimal = s.count.Decimal.Add(val)
	}
}

func (s *AvgAggregator) GetResult() (proto.Value, bool) {
	if !s.sum.Valid || !s.count.Valid {
		return nil, false
	}

	if s.count.Decimal.IsZero() {
		return nil, false
	}

	result := s.sum.Decimal.Div(s.count.Decimal)
	return proto.NewValueDecimal(result), true
}
