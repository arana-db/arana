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
	"testing"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/testdata"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

const (
	score = "score"
	age   = "age"
)

type (
	student map[string]int64
)

func TestGetCurrentRow(t *testing.T) {
	students := []student{{score: 85, age: 72}, {score: 75, age: 70}, {score: 65, age: 50}}
	rows := buildMergeRow(t, students)

	res := make([]student, 0)
	for {
		row := rows.Next()
		if row == nil {
			break
		}
		v1, _ := rows.GetCurrentRow().GetColumnValue(score)
		v2, _ := rows.GetCurrentRow().GetColumnValue(age)
		res = append(res, student{score: v1.(int64), age: v2.(int64)})
	}

	assert.Equal(t, students, res)
}

func buildMergeRow(t *testing.T, vals []student) *MergeRows {
	rows := make([]proto.Row, 0)
	for _, val := range vals {
		row := testdata.NewMockRow(gomock.NewController(t))
		for k, v := range val {
			row.EXPECT().GetColumnValue(k).Return(v, nil).AnyTimes()
		}
		rows = append(rows, row)
	}
	return NewMergeRows(rows)
}
