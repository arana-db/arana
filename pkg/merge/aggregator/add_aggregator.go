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

type AddAggregator struct {
	count decimal.NullDecimal
}

func (s *AddAggregator) Aggregate(values []proto.Value) {
	if len(values) == 0 {
		return
	}

	if values[0] == nil {
		return
	}

	val1, err := values[0].Decimal()
	if err != nil {
		panic(err.Error())
	}

	if !s.count.Valid {
		s.count.Valid = true
		s.count.Decimal = val1
		return
	}

	s.count.Decimal = s.count.Decimal.Add(val1)
}

func (s *AddAggregator) GetResult() (proto.Value, bool) {
	if !s.count.Valid {
		return nil, false
	}

	return proto.NewValueDecimal(s.count.Decimal), true
}
