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

package merge

import (
	"container/heap"
)

import (
	"github.com/arana-db/arana/pkg/merge/impl/order"
)

type PriorityQueue struct {
	rows         []*order.RowItem
	orderByItems []order.OrderByItem
}

func NewPriorityQueue(rows []*order.RowItem, orderByItems []order.OrderByItem) *PriorityQueue {
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
	orderValues1 := &order.OrderByValue{
		OrderValues: make(map[string]interface{}),
	}
	orderValues2 := &order.OrderByValue{
		OrderValues: make(map[string]interface{}),
	}
	for _, item := range pq.orderByItems {
		row1 := pq.rows[i]
		row2 := pq.rows[j]

		val1, _ := row1.Row.Get(item.Column)
		val2, _ := row2.Row.Get(item.Column)

		orderValues1.OrderValues[item.Column] = val1
		orderValues2.OrderValues[item.Column] = val2
	}
	return orderValues1.Compare(orderValues2, pq.orderByItems) > 0
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.rows[i], pq.rows[j] = pq.rows[j], pq.rows[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*order.RowItem)
	pq.rows = append(pq.rows, item)
	pq.update()
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

func (pq *PriorityQueue) update() {
	heap.Fix(pq, pq.Len()-1)
}
