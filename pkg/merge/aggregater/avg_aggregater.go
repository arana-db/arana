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

package aggregater

import (
	"fmt"
	"reflect"
)

import (
	"github.com/shopspring/decimal"
)

type AvgAggregater struct {
	sum   decimal.Decimal
	count decimal.Decimal
}

func (s *AvgAggregater) Aggregate(values []interface{}) {
	if len(values) < 2 {
		return
	}

	val1, err := parseDecimal(values[0])
	if err != nil {
		panic(err)
	}
	val2, err := parseDecimal(values[1])
	if err != nil {
		panic(err)
	}

	s.sum = s.sum.Add(val1)
	s.count = s.count.Add(val2)
}

func (s *AvgAggregater) GetResult() (decimal.Decimal, bool) {
	if s.count.IsZero() {
		return decimal.Zero, false
	}
	return s.sum.Div(s.count), true
}

func parseDecimal(val interface{}) (decimal.Decimal, error) {
	kd := reflect.TypeOf(val).Kind()

	elemPtrType := kd == reflect.Ptr
	floatType := validateFloatKind(kd)
	intType := validateIntKind(kd)
	uintType := validateUintKind(kd)

	value := reflect.ValueOf(val)
	if elemPtrType {
		value = value.Elem()
	}

	switch {
	case floatType:
		return decimal.NewFromFloat(value.Float()), nil
	case intType:
		return decimal.NewFromInt(value.Int()), nil
	case uintType:
		return decimal.NewFromInt(int64(value.Uint())), nil
	default:
		return decimal.Zero, fmt.Errorf("invalid decimal value: %v", val)
	}
}

// validateIntKind check whether k is int kind
func validateIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

// validateUintKind check whether k is uint kind
func validateUintKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	default:
		return false
	}
}

// validateFloatKind check whether k is float kind
func validateFloatKind(k reflect.Kind) bool {
	switch k {
	case reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}
