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
	"github.com/arana-db/arana/pkg/merge/impl/order"
	"io"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type orderedDatasetV2 struct {
	dataset  RandomAccessDataset
	queue    *order.OrderPriorityQueue
	firstRow bool
}

func NewOrderedDatasetV2(dataset RandomAccessDataset, items []order.OrderByItem) proto.Dataset {
	return &orderedDatasetV2{
		dataset:  dataset,
		queue:    order.NewOrderPriorityQueue(items),
		firstRow: true,
	}
}

func (or *orderedDatasetV2) Close() error {
	return or.dataset.Close()
}

func (or *orderedDatasetV2) Fields() ([]proto.Field, error) {
	return or.dataset.Fields()
}

func (or *orderedDatasetV2) Next() (proto.Row, error) {
	if or.firstRow {
		n := or.dataset.Len()
		for i := 0; i < n; i++ {
			or.dataset.SetNextN(i)
			row, err := or.dataset.Next()
			if err == io.EOF {
				continue
			} else if err != nil {
				return nil, err
			}
			a := &order.RowItem{
				Row:       row.(proto.KeyedRow),
				StreamIdx: i,
			}
			order.NewOrderByValue(a, or.queue.OrderByItems, i)
			or.queue.Push(&RowItem{
				row:       row.(proto.KeyedRow),
				streamIdx: i,
			})
		}
		or.firstRow = false
	}
	if or.queue.Len() == 0 {
		return nil, io.EOF
	}
	data := heap.Pop(or.queue)

	item := data.(*RowItem)
	or.dataset.SetNextN(item.streamIdx)
	nextRow, err := or.dataset.Next()
	if err == io.EOF {
		return item.row, nil
	} else if err != nil {
		return nil, err
	}
	heap.Push(or.queue, &RowItem{
		row:       nextRow.(proto.KeyedRow),
		streamIdx: item.streamIdx,
	})

	return item.row, nil
}
