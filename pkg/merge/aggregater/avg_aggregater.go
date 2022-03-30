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
	"fmt"
	"reflect"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"
)

type AvgAggregater struct {
	sum   gxbig.Decimal
	count gxbig.Decimal
}

func (s *AvgAggregater) Aggregate(values []interface{}) {
	if len(values) < 2 {
		return
	}

	val1, err := parseDecimal2(values[0])
	if err != nil {
		panic(err)
	}
	val2, err := parseDecimal2(values[1])
	if err != nil {
		panic(err)
	}

	gxbig.DecimalAdd(&s.sum, val1, &s.sum)
	gxbig.DecimalAdd(&s.count, val2, &s.count)
}

func (s *AvgAggregater) GetResult() (*gxbig.Decimal, bool) {
	if s.count.IsZero() {
		return nil, false
	}
	var res gxbig.Decimal
	gxbig.DecimalDiv(&s.sum, &s.count, &res, gxbig.DivFracIncr)
	return &res, true
}

func parseDecimal2(val interface{}) (*gxbig.Decimal, error) {
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
		dec := &gxbig.Decimal{}
		err := dec.FromFloat64(value.Float())
		return dec, err
	case intType:
		return gxbig.NewDecFromInt(value.Int()), nil
	case uintType:
		return gxbig.NewDecFromUint(value.Uint()), nil
	default:
		return nil, fmt.Errorf("invalid decimal value: %v", val)
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
