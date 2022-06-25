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
	"github.com/arana-db/arana/pkg/proto"
)

type OrderByStreamMergeRows struct {
	orderByItems []OrderByItem
	queue        OrderPriorityQueue
	rows         []proto.Row
	isFirstNext  bool
	currentRow   proto.Row
}

func NewOrderByStreamMergeRows(rows []proto.Row, orderByItems []OrderByItem) *OrderByStreamMergeRows {
	result := &OrderByStreamMergeRows{
		rows:         rows,
		orderByItems: orderByItems,
		queue:        buildMergeRowsToQueue(rows, orderByItems),
		isFirstNext:  true,
	}
	if result.queue.Len() > 0 {
		result.buildCurrentRow()
	}
	return result
}

func buildMergeRowsToQueue(rows []proto.Row, orderByItems []OrderByItem) OrderPriorityQueue {
	result := NewOrderPriorityQueue()
	for _, item := range rows {
		orderByValue := NewOrderByValue(item, orderByItems, 0)
		if orderByValue.Next() {
			result.Push(orderByValue)
		}
	}
	return result
}

func (rows OrderByStreamMergeRows) next() bool {
	if rows.queue.Len() == 0 {
		return false
	}
	if rows.isFirstNext {
		rows.isFirstNext = false
		return true
	}
	firstOrderByValue := rows.queue.Pop()
	if firstOrderByValue.(*OrderByValue).Next() {
		rows.queue.Push(firstOrderByValue)
	}
	if rows.queue.Len() == 0 {
		return false
	}
	rows.buildCurrentRow()
	return false
}

func (rows OrderByStreamMergeRows) buildCurrentRow() {
	item := rows.queue.Peek()
	rows.currentRow = item.(*OrderByValue).row
}
