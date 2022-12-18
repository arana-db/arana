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

type MinAggregator struct {
	min decimal.NullDecimal
}

func (s *MinAggregator) Aggregate(values []proto.Value) {
	if len(values) == 0 {
		return
	}

	if values[0] == nil {
		return
	}

	val, err := values[0].Decimal()
	if err != nil {
		panic(err)
	}

	if !s.min.Valid {
		s.min.Valid = true
		s.min.Decimal = val
		return
	}

	if s.min.Decimal.GreaterThan(val) {
		s.min.Decimal = val
	}
}

func (s *MinAggregator) GetResult() (proto.Value, bool) {
	if s.min.Valid {
		return proto.NewValueDecimal(s.min.Decimal), true
	}
	return nil, false
}
