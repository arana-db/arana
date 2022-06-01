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

package order_by

import (
	"fmt"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/merge"
	"github.com/arana-db/arana/pkg/proto"
)

type OrderByItem struct {
	Column   string
	Desc     bool
	NullDesc bool
}

type OrderByValue struct {
	row                      *merge.MergeRows
	orderByItems             []OrderByItem
	orderValuesCaseSensitive []bool
	orderValues              []interface{}
}

func NewOrderByValue(row *merge.MergeRows, orderByItems []OrderByItem) *OrderByValue {
	return &OrderByValue{
		row:                      row,
		orderByItems:             orderByItems,
		orderValuesCaseSensitive: buildOrderValuesCaseSensitive(orderByItems),
		orderValues:              make([]interface{}, len(orderByItems)),
	}
}

func buildOrderValuesCaseSensitive(orderByItems []OrderByItem) []bool {
	// TODO Get Database case sensitive from schema metadata.
	result := make([]bool, len(orderByItems))
	return result
}

func (value *OrderByValue) Next() bool {
	result := value.row.HasNext()
	value.buildOrderValues()
	return result
}

func (value *OrderByValue) buildOrderValues() {
	for _, item := range value.orderByItems {
		val, err := value.row.GetCurrentRow().(proto.KeyedRow).Get(item.Column)
		if err != nil {
			panic("get order by column value error:" + err.Error())
		}
		// TODO How to judgment the value is Comparable.
		//if val != nil && false {
		//	panic("get order by column value error:" + err.Error())
		//}
		value.orderValues = append(value.orderValues, val)
	}
}

func (value *OrderByValue) Compare(compareVal *OrderByValue) int8 {
	for i, item := range value.orderByItems {
		compare := compareTo(value.orderValues[i], compareVal.orderValues[i], item.NullDesc, item.Desc, value.orderValuesCaseSensitive[i])
		if compare == 0 {
			continue
		}
		return compare
	}
	return 0
}

func compareTo(thisValue, otherValue interface{}, nullDesc, desc, caseSensitive bool) int8 {
	if thisValue == nil && otherValue == nil {
		return 0
	}
	if thisValue == nil {
		if nullDesc {
			return -1
		}
		return 1
	}
	if otherValue == nil {
		if nullDesc {
			return 1
		}
		return -1
	}
	// TODO Deal with case sensitive.
	switch thisValue.(type) {
	case string:
		return compareString(fmt.Sprintf("%v", thisValue), fmt.Sprintf("%v", otherValue), desc)
	case int8, int16, int32, int64:
		return compareInt64(thisValue.(int64), otherValue.(int64), desc)
	case uint8, uint16, uint32, uint64:
		return compareUint64(thisValue.(uint64), otherValue.(uint64), desc)
	case float32, float64:
		return compareFloat64(thisValue.(float64), otherValue.(float64), desc)
	case time.Time:
		return compareTime(thisValue.(time.Time), otherValue.(time.Time), desc)
	}
	return 0
}

func compareTime(thisValue, otherValue time.Time, desc bool) int8 {
	if desc {
		if thisValue.After(otherValue) {
			return -1
		}
		return 1
	} else {
		if thisValue.After(otherValue) {
			return 1
		}
		return -1
	}
}

func compareString(thisValue, otherValue string, desc bool) int8 {
	if desc {
		if fmt.Sprintf("%v", thisValue) > fmt.Sprintf("%v", otherValue) {
			return -1
		}
		return 1
	} else {
		if fmt.Sprintf("%v", thisValue) > fmt.Sprintf("%v", otherValue) {
			return 1
		}
		return -1
	}
}

func compareInt64(thisValue, otherValue int64, desc bool) int8 {
	if desc {
		if thisValue > otherValue {
			return -1
		}
		return 1
	} else {
		if thisValue > otherValue {
			return 1
		}
		return -1
	}
}

func compareUint64(thisValue, otherValue uint64, desc bool) int8 {
	if desc {
		if thisValue > otherValue {
			return -1
		}
		return 1
	} else {
		if thisValue > otherValue {
			return 1
		}
		return -1
	}
}

func compareFloat64(thisValue, otherValue float64, desc bool) int8 {
	if desc {
		if thisValue > otherValue {
			return -1
		}
		return 1
	} else {
		if thisValue > otherValue {
			return 1
		}
		return -1
	}
}
