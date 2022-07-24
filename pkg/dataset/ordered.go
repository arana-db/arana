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
	"io"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type orderedDataset struct {
	dataset  RandomAccessDataset
	queue    *PriorityQueue
	firstRow bool
}

func NewOrderedDataset(dataset RandomAccessDataset, items []OrderByItem) proto.Dataset {
	return &orderedDataset{
		dataset:  dataset,
		queue:    NewPriorityQueue(make([]*RowItem, 0), items),
		firstRow: true,
	}
}

func (or *orderedDataset) Close() error {
	return or.dataset.Close()
}

func (or *orderedDataset) Fields() ([]proto.Field, error) {
	return or.dataset.Fields()
}

func (or *orderedDataset) Next() (proto.Row, error) {
	if or.firstRow {
		or.firstRow = false
		n := or.dataset.Len()
		for i := 0; i < n; i++ {
			_ = or.dataset.SetNextN(i)
			row, err := or.dataset.Next()
			if err == io.EOF {
				continue
			} else if err != nil {
				return nil, err
			}
			or.queue.Push(&RowItem{
				row:       row.(proto.KeyedRow),
				streamIdx: i,
			})
		}
	}

	if or.queue.Len() == 0 {
		return nil, io.EOF
	}
	data := heap.Pop(or.queue)

	item := data.(*RowItem)
	_ = or.dataset.SetNextN(item.streamIdx)
	nextRow, err := or.dataset.Next()
	if errors.Is(err, io.EOF) {
		return item.row, nil
	}
	if err != nil {
		return nil, err
	}

	heap.Push(or.queue, &RowItem{
		row:       nextRow.(proto.KeyedRow),
		streamIdx: item.streamIdx,
	})

	return item.row, nil
}
