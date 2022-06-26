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

package order

import (
	"fmt"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type OrderByValue struct {
	OrderValues map[string]interface{}
}

type RowItem struct {
	Row       proto.KeyedRow
	StreamIdx int
}
type OrderByItem struct {
	Column string
	Desc   bool
}

func (value *OrderByValue) Compare(compareVal *OrderByValue, orderByItems []OrderByItem) int8 {
	for _, item := range orderByItems {
		compare := compareTo(value.OrderValues[item.Column], compareVal.OrderValues[item.Column], item.Desc)
		if compare == 0 {
			continue
		}
		return compare
	}
	return 0
}

func compareTo(thisValue, otherValue interface{}, desc bool) int8 {
	if thisValue == nil && otherValue == nil {
		return 0
	}
	if thisValue == nil {
		return 1
	}
	if otherValue == nil {
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
			return 1
		}
		return -1
	} else {
		if thisValue.After(otherValue) {
			return -1
		}
		return 1
	}
}

func compareString(thisValue, otherValue string, desc bool) int8 {
	if desc {
		if fmt.Sprintf("%v", thisValue) > fmt.Sprintf("%v", otherValue) {
			return 1
		}
		return -1
	} else {
		if fmt.Sprintf("%v", thisValue) > fmt.Sprintf("%v", otherValue) {
			return -1
		}
		return 1
	}
}

func compareInt64(thisValue, otherValue int64, desc bool) int8 {
	if desc {
		if thisValue > otherValue {
			return 1
		}
		return -1
	} else {
		if thisValue > otherValue {
			return -1
		}
		return 1
	}
}

func compareUint64(thisValue, otherValue uint64, desc bool) int8 {
	if desc {
		if thisValue > otherValue {
			return 1
		}
		return -1
	} else {
		if thisValue > otherValue {
			return -1
		}
		return 1
	}
}

func compareFloat64(thisValue, otherValue float64, desc bool) int8 {
	if desc {
		if thisValue > otherValue {
			return 1
		}
		return -1
	} else {
		if thisValue > otherValue {
			return -1
		}
		return 1
	}
}
