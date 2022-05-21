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

package group_by

import (
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/merge"
	order "github.com/arana-db/arana/pkg/merge/impl/order_by"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/testdata"
)

const (
	countScore = "count(score)"
	age        = "age"
)

type (
	student map[string]int64
)

func TestGroupByStreamMergeRows(t *testing.T) {

	stmt := MergeRowStatement{
		OrderBys: []order.OrderByItem{
			{
				Column: age,
				Desc:   true,
			},
		},
		Selects: []SelectItem{
			{
				Column:       countScore,
				AggrFunction: ast.AggrSum,
			},
			{
				Column: age,
			},
		},
		GroupBys: []string{age},
	}
	rows := buildMergeRows(t, [][]student{
		{{countScore: 85, age: 81}, {countScore: 75, age: 70}, {countScore: 65, age: 60}},
		{{countScore: 90, age: 81}, {countScore: 75, age: 68}, {countScore: 70, age: 40}},
		{{countScore: 85, age: 70}, {countScore: 78, age: 60}},
	})

	mergeRow := NewGroupByStreamMergeRow(rows, stmt)

	res := make([]student, 0)
	for {
		row := mergeRow.Next()
		if row == nil {
			break
		}
		v1, _ := row.GetColumnValue(countScore)
		v2, _ := row.GetColumnValue(age)
		res = append(res, student{countScore: v1.(int64), age: v2.(int64)})
	}
	assert.Equal(t, []student{
		{countScore: 175, age: 81}, {countScore: 160, age: 70}, {countScore: 75, age: 68},
		{countScore: 143, age: 60}, {countScore: 70, age: 40},
	}, res)
}

func buildMergeRows(t *testing.T, vals [][]student) []*merge.MergeRows {
	rows := make([]*merge.MergeRows, 0)
	for _, v := range vals {
		rows = append(rows, buildMergeRow(t, v))
	}
	return rows
}

func buildMergeRow(t *testing.T, vals []student) *merge.MergeRows {
	rows := make([]proto.Row, 0)
	for _, val := range vals {
		row := testdata.NewMockRow(gomock.NewController(t))
		for k, v := range val {
			row.EXPECT().GetColumnValue(k).Return(v, nil).AnyTimes()
		}
		rows = append(rows, row)
	}
	return merge.NewMergeRows(rows)
}
