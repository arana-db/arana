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

package group

import (
	"container/heap"
	"fmt"
)

import (
	"github.com/golang/mock/gomock"
)

import (
	"github.com/arana-db/arana/pkg/merge"
	"github.com/arana-db/arana/pkg/merge/aggregator"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/testdata"
)

// MergeRowStatement represents merge-row statement.
// Please see the design documents:
//   https://dubbo-kylin.yuque.com/docs/share/ff2e78b8-df2c-4874-b26e-cb6b923033b8
type MergeRowStatement struct {
	OrderBys []merge.OrderByItem
	GroupBys []string
	Selects  []SelectItem
}

type SelectItem struct {
	Column       string
	AggrFunction string
}

type GroupByStreamMergeRows struct {
	stmt         MergeRowStatement
	queue        merge.PriorityQueue
	currentRow   proto.Row
	isFirstMerge bool
}

func NewGroupByStreamMergeRow(rows []*merge.MergeRows, stmt MergeRowStatement) *GroupByStreamMergeRows {
	s := &GroupByStreamMergeRows{
		stmt:         stmt,
		queue:        merge.NewPriorityQueue(rows, stmt.OrderBys),
		isFirstMerge: true,
	}
	return s
}

func (s *GroupByStreamMergeRows) Next() proto.Row {
	return s.merge()
}

func (s *GroupByStreamMergeRows) getAggrUnitMap(stmt MergeRowStatement) map[string]merge.Aggregator {
	aggrMap := make(map[string]merge.Aggregator, 0)
	for _, sel := range stmt.Selects {
		if sel.AggrFunction != "" {
			aggrMap[sel.Column] = s.getAggregator(sel.AggrFunction)
		}
	}
	return aggrMap
}

func (s *GroupByStreamMergeRows) getAggregator(aggrName string) merge.Aggregator {
	switch aggrName {
	case ast.AggrAvg:
		return new(aggregator.AvgAggregator)
	case ast.AggrMax:
		return new(aggregator.MaxAggregator)
	case ast.AggrMin:
		return new(aggregator.MinAggregator)
	case ast.AggrSum, ast.AggrCount:
		return new(aggregator.AddAggregator)
	default:
		panic(fmt.Errorf("unsupported aggr source type: %s", aggrName))
	}
}

func (s *GroupByStreamMergeRows) merge() proto.Row {
	if s.queue.Len() == 0 {
		return nil
	}
	if s.isFirstMerge {
		s.currentRow = s.queue.Peek().(*merge.MergeRows).GetCurrentRow()
		s.isFirstMerge = false
	}
	aggrMap := s.getAggrUnitMap(s.stmt)
	currentRow := s.currentRow
	groupByColumns := make([]string, 0, len(s.stmt.GroupBys))
	groupByColumns = append(groupByColumns, s.stmt.GroupBys...)
	currentGroupValue := NewGroupByValue(groupByColumns, currentRow)
	for {
		if currentGroupValue.equals(s.currentRow) {
			s.aggregate(aggrMap, s.currentRow)
		} else {
			break
		}
		if !s.hasNext() {
			break
		}
	}

	row := testdata.NewMockKeyedRow(gomock.NewController(nil))
	for _, sel := range s.stmt.Selects {
		if _, ok := aggrMap[sel.Column]; ok {
			res, _ := aggrMap[sel.Column].GetResult()
			// TODO use row encode() to build a new row result
			val, _ := res.ToInt()
			row.EXPECT().Get(sel.Column).Return(val, nil).AnyTimes()
		} else {
			res, _ := currentRow.(proto.KeyedRow).Get(sel.Column)
			row.EXPECT().Get(sel.Column).Return(res, nil).AnyTimes()
		}
	}
	return row
}

// todo not support Avg method yet
func (s *GroupByStreamMergeRows) aggregate(aggrMap map[string]merge.Aggregator, row proto.Row) {
	for k, v := range aggrMap {
		val, err := row.(proto.KeyedRow).Get(k)
		if err != nil {
			panic(err.Error())
		}
		v.Aggregate([]interface{}{val})
	}
}

func (s *GroupByStreamMergeRows) hasNext() bool {
	// s.currentMergedRow = nil
	s.currentRow = nil
	if s.queue.Len() == 0 {
		return false
	}

	hp := heap.Pop(&s.queue).(*merge.MergeRows)
	if (*hp).Next() != nil {
		s.queue.Push(hp)
	}
	if s.queue.Len() == 0 {
		return false
	}
	s.currentRow = s.queue.Peek().(*merge.MergeRows).GetCurrentRow()
	return true
}

func (s *GroupByStreamMergeRows) GetCurrentRow() proto.Row {
	return s.currentRow
}
