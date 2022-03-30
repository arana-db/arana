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
	"fmt"
)

type PriorityQueue struct {
	results      []*MergeRows
	orderByItems []OrderByItem
}

type OrderByItem struct {
	Column string
	Desc   bool
}

func NewPriorityQueue(rows []*MergeRows, orderByItems []OrderByItem) PriorityQueue {
	pq := PriorityQueue{
		results:      rows,
		orderByItems: orderByItems,
	}
	heap.Init(&pq)
	return pq
}

func (pq *PriorityQueue) Len() int {
	return len(pq.results)
}

func (pq *PriorityQueue) Less(i, j int) bool {
	for _, item := range pq.orderByItems {
		val1, _ := pq.results[i].GetCurrentRow().GetColumnValue(item.Column)
		val2, _ := pq.results[j].GetCurrentRow().GetColumnValue(item.Column)
		if val1 != val2 {
			if item.Desc {
				return fmt.Sprintf("%v", val1) > fmt.Sprintf("%v", val2)
			}
			return fmt.Sprintf("%v", val1) < fmt.Sprintf("%v", val2)
		}
	}
	return true
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.results[i], pq.results[j] = pq.results[j], pq.results[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*MergeRows)
	pq.results = append(pq.results, item)
	pq.update()
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old.results)
	item := old.results[n-1]
	pq.results = old.results[0 : n-1]
	pq.update()
	return item
}

func (pq *PriorityQueue) Peek() interface{} {
	return pq.results[0]
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update() {
	heap.Fix(pq, len(pq.results)-1)
}
