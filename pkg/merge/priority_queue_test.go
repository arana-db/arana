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

package merge

import (
	"container/heap"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestPriorityQueue(t *testing.T) {
	rows := buildMergeRows(t, [][]student{
		{{score: 85, age: 72}, {score: 75, age: 70}, {score: 65, age: 50}},
		{{score: 90, age: 72}, {score: 75, age: 68}, {score: 70, age: 40}},
		{{score: 85, age: 70}, {score: 78, age: 60}, {score: 75, age: 80}, {score: 65, age: 80}, {score: 60, age: 40}},
	})
	queue := NewPriorityQueue(rows, []OrderByItem{
		{Column: score, Desc: true},
		{Column: age, Desc: true},
	})

	res := make([]student, 0)
	for {
		if queue.Len() == 0 {
			break
		}
		row := heap.Pop(&queue).(*MergeRows)
		v1, _ := row.GetCurrentRow().GetColumnValue(score)
		v2, _ := row.GetCurrentRow().GetColumnValue(age)
		res = append(res, student{score: v1.(int64), age: v2.(int64)})
		if row.Next() != nil {
			queue.Push(row)
		}
	}
	assert.Equal(t, []student{
		{score: 90, age: 72}, {score: 85, age: 72}, {score: 85, age: 70},
		{score: 78, age: 60}, {score: 75, age: 80}, {score: 75, age: 70},
		{score: 75, age: 68}, {score: 70, age: 40}, {score: 65, age: 80},
		{score: 65, age: 50}, {score: 60, age: 40},
	}, res)
}

func TestPriorityQueue2(t *testing.T) {
	rows := buildMergeRows(t, [][]student{
		{{score: 65, age: 50}, {score: 75, age: 70}, {score: 85, age: 72}},
		{{score: 70, age: 40}, {score: 75, age: 68}, {score: 90, age: 72}},
		{{score: 60, age: 40}, {score: 65, age: 80}, {score: 75, age: 80}, {score: 78, age: 60}, {score: 85, age: 70}},
	})
	queue := NewPriorityQueue(rows, []OrderByItem{
		{Column: score, Desc: false},
		{Column: age, Desc: false},
	})

	res := make([]student, 0)
	for {
		if queue.Len() == 0 {
			break
		}
		row := heap.Pop(&queue).(*MergeRows)
		v1, _ := row.GetCurrentRow().GetColumnValue(score)
		v2, _ := row.GetCurrentRow().GetColumnValue(age)
		res = append(res, student{score: v1.(int64), age: v2.(int64)})
		if row.Next() != nil {
			queue.Push(row)
		}
	}
	assert.Equal(t, []student{
		{score: 60, age: 40}, {score: 65, age: 50}, {score: 65, age: 80},
		{score: 70, age: 40}, {score: 75, age: 68}, {score: 75, age: 70},
		{score: 75, age: 80}, {score: 78, age: 60}, {score: 85, age: 70},
		{score: 85, age: 72}, {score: 90, age: 72},
	}, res)
}

func buildMergeRows(t *testing.T, vals [][]student) []*MergeRows {
	rows := make([]*MergeRows, 0)
	for _, v := range vals {
		rows = append(rows, buildMergeRow(t, v))
	}
	return rows
}
