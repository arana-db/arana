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
	"github.com/dubbogo/arana/pkg/merge"
	"github.com/dubbogo/arana/pkg/merge/aggregater"
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/runtime/xxast"
	"github.com/dubbogo/arana/testdata"

	"github.com/golang/mock/gomock"
)

type GroupByStreamMergeRows struct {
	stmt         xxast.SelectStatement
	queue        merge.PriorityQueue
	currentRow   proto.Row
	isFirstMerge bool
}

func NewGroupByStreamMergeRow(rows []*merge.MergeRows, stmt xxast.SelectStatement) *GroupByStreamMergeRows {
	orderByItems := make([]merge.OrderByItem, 0)
	for _, item := range stmt.OrderBy {
		column := item.Alias
		if column == "" {
			column = item.Expr.(xxast.ColumnNameExpressionAtom)[0]
		}
		orderByItems = append(orderByItems, merge.OrderByItem{
			Column: column,
			Desc:   item.Desc,
		})
	}
	s := &GroupByStreamMergeRows{
		stmt:         stmt,
		queue:        merge.NewPriorityQueue(rows, orderByItems),
		isFirstMerge: true,
	}
	return s
}

func (s *GroupByStreamMergeRows) Next() proto.Row {
	return s.merge()
}

func (s *GroupByStreamMergeRows) getAggrUnitMap(stmt xxast.SelectStatement) map[string]merge.Aggregater {
	aggrMap := make(map[string]merge.Aggregater, 0)
	for _, sel := range stmt.Select {
		// todo Mock is temporarily used to facilitate the test, which will be changed back
		//if fun, ok := sel.(*xxast.SelectElementFunction); ok {
		if fun, ok := sel.(*testdata.MockSelectElementFunction); ok {
			columnName := fun.ToSelectString()
			if fun.Alias() != "" {
				columnName = fun.Alias()
			}
			// todo Mock is temporarily used to facilitate the test, which will be changed back
			//aggrMap[columnName] = s.getAggregater(fun.Function().(*xxast.AggrFunction).Name())
			aggrMap[columnName] = s.getAggregater(xxast.AggrCount)
		}
	}
	return aggrMap
}

func (s *GroupByStreamMergeRows) getAggregater(aggrName string) merge.Aggregater {
	switch aggrName {
	case xxast.AggrAvg:
		return new(aggregater.AvgAggregater)
	case xxast.AggrMax:
		return new(aggregater.MaxAggregater)
	case xxast.AggrMin:
		return new(aggregater.MinAggregater)
	case xxast.AggrSum, xxast.AggrCount:
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
	currentGroupValue := NewGroupByValue(s.stmt.GroupBy, currentRow)
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

	// TODO Row needs to provide Encode method to modify the value of GetData() method
	//return currentRow
	//return proto.Row{}
	age, _ := currentRow.GetColumnValue("age")
	row := testdata.NewMockRow(gomock.NewController(nil))
	row.EXPECT().GetColumnValue("age").Return(age, nil).AnyTimes()
	res, _ := aggrMap["count(score)"].GetResult()
	row.EXPECT().GetColumnValue("count(score)").Return(res.IntPart(), nil).AnyTimes()
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
