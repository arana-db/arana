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
	"github.com/arana-db/arana/pkg/proto"
)

var _ proto.Dataset = (*SortMergeJoin)(nil)

type SortMergeJoin struct {
	fields       []proto.Field
	joinColumn   *JoinColumn
	leftDataset  proto.Dataset
	rightDataset proto.Dataset
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
	// left or right dataset is nil, direct ret
	// get left dataset data , get right dataset data
	//	init a cursor with left data
	// 		if left data join key equal right return data
	//		if left data less than right data , lost left data
	//		if left data great than right data, lost right data, cursor to right data

	leftRow, err := s.getLeftRow()
	if err != nil {
		return nil, err
	}

	rightRow, err := s.getRightRow()
	if err != nil {
		return nil, err
	}

	var leftValue, rightValue proto.Value

	for {
		if leftRow == nil || rightRow == nil {
			return nil, nil
		}

		leftValue, err = leftRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		rightValue, err = rightRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		if leftValue.String() == "" || rightValue.String() == "" {
			return nil, nil
		}

		if strings.Compare(leftValue.String(), rightValue.String()) == 0 {
			return leftRow, nil
		}

		if strings.Compare(leftValue.String(), rightValue.String()) < 0 {
			// get next left data
			leftRow, err = s.getLeftRow()
			if err != nil {
				return nil, nil
			}
		}

		if strings.Compare(leftValue.String(), rightValue.String()) > 0 {
			// get next right data
			rightRow, err = s.getRightRow()
			if err != nil {
				return nil, err
			}
		}
	}
}

func (s *SortMergeJoin) getLeftRow() (proto.Row, error) {
	leftRow, err := s.leftDataset.Next()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return leftRow, nil
}

func (s *SortMergeJoin) getRightRow() (proto.Row, error) {
	rightRow, err := s.rightDataset.Next()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return rightRow, nil
}
