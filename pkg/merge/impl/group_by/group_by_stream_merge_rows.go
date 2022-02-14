//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package group_by

import (
	"container/heap"
	"fmt"
)

import (
	"github.com/golang/mock/gomock"
)

import (
	"github.com/dubbogo/arana/pkg/merge"
	"github.com/dubbogo/arana/pkg/merge/aggregater"
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/runtime/ast"
	"github.com/dubbogo/arana/testdata"
)

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

func (s *GroupByStreamMergeRows) getAggrUnitMap(stmt MergeRowStatement) map[string]merge.Aggregater {
	aggrMap := make(map[string]merge.Aggregater, 0)
	for _, sel := range stmt.Selects {
		if sel.AggrFunction != "" {
			aggrMap[sel.Column] = s.getAggregater(sel.AggrFunction)
		}
	}
	return aggrMap
}

func (s *GroupByStreamMergeRows) getAggregater(aggrName string) merge.Aggregater {
	switch aggrName {
	case ast.AggrAvg:
		return new(aggregater.AvgAggregater)
	case ast.AggrMax:
		return new(aggregater.MaxAggregater)
	case ast.AggrMin:
		return new(aggregater.MinAggregater)
	case ast.AggrSum, ast.AggrCount:
		return new(aggregater.AddAggregater)
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
	groupByColumns := make([]string, 0)
	for _, item := range s.stmt.GroupBys {
		groupByColumns = append(groupByColumns, item)
	}

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

	row := testdata.NewMockRow(gomock.NewController(nil))
	for _, sel := range s.stmt.Selects {
		if _, ok := aggrMap[sel.Column]; ok {
			res, _ := aggrMap[sel.Column].GetResult()
			// TODO use row encode() to build a new row result
			row.EXPECT().GetColumnValue(sel.Column).Return(res.IntPart(), nil).AnyTimes()
		} else {
			res, _ := currentRow.GetColumnValue(sel.Column)
			row.EXPECT().GetColumnValue(sel.Column).Return(res, nil).AnyTimes()
		}
	}
	return row
}

// todo not support Avg method yet
func (s *GroupByStreamMergeRows) aggregate(aggrMap map[string]merge.Aggregater, row proto.Row) {
	for k, v := range aggrMap {
		val, err := row.GetColumnValue(k)
		if err != nil {
			panic(err.Error())
		}
		v.Aggregate([]interface{}{val})
	}
}

func (s *GroupByStreamMergeRows) hasNext() bool {
	//s.currentMergedRow = nil
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
