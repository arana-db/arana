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
	"io"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Dataset = (*SortMergeJoin)(nil)

type SortMergeJoin struct {
	fields     []proto.Field
	joinColumn *JoinColumn
	joinType   ast.JoinType
	outer      proto.Dataset
	inner      proto.Dataset
}

type JoinColumn struct {
	column string
}

func (j *JoinColumn) Column() string {
	if j != nil {
		return j.column
	}

	return ""
}

func (s *SortMergeJoin) Close() error {
	return nil
}

func (s *SortMergeJoin) Fields() ([]proto.Field, error) {
	return s.fields, nil
}

func (s *SortMergeJoin) Next() (proto.Row, error) {
	// all data is order

	outerRow, err := s.getOuterRow()
	if err != nil {
		return nil, err
	}

	innerRow, err := s.getInnerRow()
	if err != nil {
		return nil, err
	}

	var outerValue, innerValue proto.Value

	for {
		if outerRow == nil && innerRow == nil {
			return nil, nil
		}

		outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		innerValue, err = innerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		if strings.Compare(outerValue.String(), innerValue.String()) == 0 {
			return s.resGenerate(outerRow, innerRow), nil
		}

		if strings.Compare(outerValue.String(), innerValue.String()) < 0 {
			return s.resGenerate(outerRow, nil), nil
		}

		if strings.Compare(outerValue.String(), innerValue.String()) > 0 {
			innerRow, err = s.getInnerRow()
			if err != nil {
				return nil, err
			}
		}
	}
}

func (s *SortMergeJoin) getOuterRow() (proto.Row, error) {
	leftRow, err := s.outer.Next()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return leftRow, nil
}

func (s *SortMergeJoin) getInnerRow() (proto.Row, error) {
	rightRow, err := s.inner.Next()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return rightRow, nil
}

func (s *SortMergeJoin) resGenerate(leftRow proto.Row, rightRow proto.Row) proto.Row {
	res := make([]proto.Value, leftRow.Length()+rightRow.Length())
	leftValue := make([]proto.Value, leftRow.Length())
	rightValue := make([]proto.Value, rightRow.Length())

	_ = leftRow.Scan(leftValue)
	_ = rightRow.Scan(rightValue)

	res = append(res, leftValue...)
	res = append(res, rightValue...)

	return rows.NewBinaryVirtualRow(s.fields, res)
}
