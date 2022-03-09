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
	"github.com/dubbogo/arana/pkg/proto"
)

type MergeRows struct {
	rows            []proto.Row
	currentRowIndex int
}

func NewMergeRows(rows []proto.Row) *MergeRows {
	return &MergeRows{rows: rows, currentRowIndex: -1}
}

func NewMergeRowses(rowses [][]proto.Row) []*MergeRows {
	ss := make([]*MergeRows, 0)
	for _, rows := range rowses {
		ss = append(ss, NewMergeRows(rows))
	}
	return ss
}

func (s *MergeRows) Next() proto.Row {
	if len(s.rows) == 0 || s.currentRowIndex >= len(s.rows)-1 {
		return nil
	}
	s.currentRowIndex++
	result := s.rows[s.currentRowIndex]
	return result
}

func (s *MergeRows) GetCurrentRow() proto.Row {
	if len(s.rows) == 0 || s.currentRowIndex > len(s.rows) {
		return nil
	}
	if s.currentRowIndex < 0 {
		s.Next()
	}
	return s.rows[s.currentRowIndex]
}
