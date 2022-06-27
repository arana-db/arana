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

package dataset

import (
	"container/heap"
	"fmt"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type PriorityQueue struct {
	rows         []*RowItem
	orderByItems []OrderByItem
}

type RowItem struct {
	row       proto.KeyedRow
	streamIdx int
}
type OrderByItem struct {
	Column string
	Desc   bool
}

type orderByValue struct {
	orderValues map[string]interface{}
}

func NewPriorityQueue(rows []*RowItem, orderByItems []OrderByItem) *PriorityQueue {
	pq := &PriorityQueue{
		rows:         rows,
		orderByItems: orderByItems,
	}
	heap.Init(pq)
	return pq
}

func (pq *PriorityQueue) Len() int {
	return len(pq.rows)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	orderValues1 := &orderByValue{
		orderValues: make(map[string]interface{}),
	}
	orderValues2 := &orderByValue{
		orderValues: make(map[string]interface{}),
	}
	for _, item := range pq.orderByItems {
		row1 := pq.rows[i]
		row2 := pq.rows[j]

		val1, _ := row1.row.Get(item.Column)
		val2, _ := row2.row.Get(item.Column)
		orderValues1.orderValues[item.Column] = val1
		orderValues2.orderValues[item.Column] = val2
	}
	return orderValues1.Compare(orderValues2, pq.orderByItems) > 0
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.rows[i], pq.rows[j] = pq.rows[j], pq.rows[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*RowItem)
	pq.rows = append(pq.rows, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old.rows)
	if n == 0 {
		return nil
	}
	item := old.rows[n-1]
	pq.rows = old.rows[0 : n-1]
	return item
}

func (value *orderByValue) Compare(compareVal *orderByValue, orderByItems []OrderByItem) int {
	for _, item := range orderByItems {
		compare := compareTo(value.orderValues[item.Column], compareVal.orderValues[item.Column], item.Desc)
		if compare == 0 {
			continue
		}
		return compare
	}
	return 0
}

func compareTo(thisValue, otherValue interface{}, desc bool) int {
	if thisValue == nil && otherValue == nil {
		return 0
	}
	if thisValue == nil {
		return 1
	}
	if otherValue == nil {
		return -1
	}
	var (
		result = 0
	)
	// TODO Deal with case sensitive.
	switch thisValue.(type) {
	case string:
		result = compareString(fmt.Sprintf("%v", thisValue), fmt.Sprintf("%v", otherValue))
	case int8, int16, int32, int64:
		result = compareInt64(thisValue.(int64), otherValue.(int64))
	case uint8, uint16, uint32, uint64:
		result = compareUint64(thisValue.(uint64), otherValue.(uint64))
	case float32, float64:
		result = compareFloat64(thisValue.(float64), otherValue.(float64))
	case time.Time:
		result = compareTime(thisValue.(time.Time), otherValue.(time.Time))
	}
	if desc {
		return -1 * result
	}
	return result
}

func compareTime(thisValue, otherValue time.Time) int {
	if thisValue.After(otherValue) {
		return -1
	}
	return 1
}

func compareString(thisValue, otherValue string) int {
	if fmt.Sprintf("%v", thisValue) > fmt.Sprintf("%v", otherValue) {
		return -1
	}
	return 1
}

func compareInt64(thisValue, otherValue int64) int {
	if thisValue > otherValue {
		return -1
	}
	return 1
}

func compareUint64(thisValue, otherValue uint64) int {
	if thisValue > otherValue {
		return -1
	}
	return 1
}

func compareFloat64(thisValue, otherValue float64) int {
	if thisValue > otherValue {
		return -1
	}
	return 1
}
