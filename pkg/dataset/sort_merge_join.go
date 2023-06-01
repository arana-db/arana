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
	"go.uber.org/atomic"
	"io"
	"strings"
	"sync"
)

import (
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

import (
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Dataset = (*SortMergeJoin)(nil)

const (
	IsDescartes  = true
	NotDescartes = false
)

// SortMergeJoin assume all outer data and inner data are sorted by join column
type SortMergeJoin struct {
	// fields is the union of outer fields and inner fields
	fields     []proto.Field
	joinColumn *JoinColumn
	// joinType inner join, left join, right join
	joinType  ast.JoinType
	outer     proto.Dataset
	inner     proto.Dataset
	beforeRow proto.Row
	// equalValue when outer value equal inner value, set this value use to generate descartes product
	equalValue map[string][]proto.Row
	// equalIndex record the index of equalValue visited position
	equalIndex map[string]int
	// descartesFlag
	descartesFlag atomic.Bool
	rwLock        sync.RWMutex
}

func NewSortMergeJoin(joinType ast.JoinType, joinColumn *JoinColumn, outer proto.Dataset, inner proto.Dataset) (*SortMergeJoin, error) {
	outerFields, err := outer.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	innerFields, err := inner.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields := make([]proto.Field, 0, len(outerFields)+len(innerFields))
	fields = append(fields, outerFields...)
	fields = append(fields, innerFields...)

	return &SortMergeJoin{
		fields:     fields,
		joinColumn: joinColumn,
		joinType:   joinType,
		outer:      outer,
		inner:      inner,
		equalValue: make(map[string][]proto.Row),
		equalIndex: make(map[string]int),
	}, nil
}

func (s *SortMergeJoin) SetBeforeRow(v proto.Row) {
	if s != nil {
		s.beforeRow = v
	}
}

func (s *SortMergeJoin) BeforeRow() proto.Row {
	if s != nil {
		return s.beforeRow
	}

	return nil
}

func (s *SortMergeJoin) CleanBeforeRow() {
	if s != nil {
		s.beforeRow = nil
	}
}

func (s *SortMergeJoin) SetEqualValue(key string, value proto.Row) {
	if s != nil {
		s.rwLock.Lock()
		defer s.rwLock.Unlock()
		s.equalValue[key] = append(s.equalValue[key], value)
		if _, ok := s.equalIndex[key]; !ok {
			s.equalIndex[key] = 0
		}
	}
}

func (s *SortMergeJoin) EqualValue(key string) proto.Row {
	if s != nil {
		s.rwLock.RLock()
		defer s.rwLock.RUnlock()

		if v, ok := s.equalValue[key]; ok {
			res := v[s.equalIndex[key]]
			index := s.equalIndex[key] + 1
			if index < len(v) {
				s.equalIndex[key] = index
			} else {
				s.equalIndex[key] = 0
			}
			return res
		}
		return nil
	}

	return nil
}

func (s *SortMergeJoin) EqualValueLen(key string) int {
	if s != nil {
		s.rwLock.RLock()
		defer s.rwLock.RUnlock()

		if v, ok := s.equalValue[key]; ok {
			return len(v)
		}
	}

	return 0
}

func (s *SortMergeJoin) EqualIndex(key string) int {
	if s != nil {
		s.rwLock.RLock()
		defer s.rwLock.RUnlock()

		if v, ok := s.equalIndex[key]; ok {
			return v
		}
	}

	return 0
}

func (s *SortMergeJoin) isDescartes(key string) bool {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	if _, ok := s.equalValue[key]; ok {
		return true
	}

	return false
}

func (s *SortMergeJoin) setDescartesFlag(flag bool) {
	if s != nil {
		s.descartesFlag.Store(flag)
	}
}

func (s *SortMergeJoin) DescartesFlag() bool {
	if s != nil {
		return s.descartesFlag.Load()
	}

	return false
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
	var (
		err                error
		outerRow, innerRow proto.Row
	)

	if s.BeforeRow() != nil {
		outerRow = s.BeforeRow()
	} else {
		outerRow, err = s.getOuterRow()
		if err != nil {
			return nil, err
		}
	}

	innerRow, err = s.getInnerRow(outerRow)
	if err != nil {
		return nil, err
	}

	switch s.joinType {
	case ast.InnerJoin:
		return s.innerJoin(outerRow, innerRow)
	case ast.LeftJoin:
		return s.leftJoin(outerRow, innerRow)
	case ast.RightJoin:
		return s.rightJoin()
	default:
		return nil, errors.New("not support join type")
	}
}

// innerJoin
func (s *SortMergeJoin) innerJoin(outerRow proto.Row, innerRow proto.Row) (proto.Row, error) {
	var (
		err                    error
		outerValue, innerValue proto.Value
	)

	for {
		if outerRow == nil || innerRow == nil {
			return nil, io.EOF
		}

		outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		innerValue, err = innerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		if proto.CompareValue(outerValue, innerValue) == 0 {
			// restore last value
			// example : 1,2,2 => 2,2,3
			// first left 2 match right first 2
			// next still need left 2  match next second 2
			s.SetBeforeRow(outerRow)
			if s.DescartesFlag() {
				index := s.EqualIndex(outerValue.String())
				if index == 0 {
					nextOuterRow, err := s.getOuterRow()
					if err != nil {
						return nil, err
					}
					s.SetBeforeRow(nextOuterRow)
				}
			}

			if !s.DescartesFlag() {
				s.SetEqualValue(cast.ToString(outerValue), innerRow)
			}

			return s.resGenerate(outerRow, innerRow), nil
		}

		if proto.CompareValue(outerValue, innerValue) < 0 {
			s.CleanBeforeRow()
			outerRow, err = s.getOuterRow()
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}

			// if outer row equal last row, do descartes match
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			s.setDescartesFlag(NotDescartes)
			if s.isDescartes(outerValue.String()) {
				s.setDescartesFlag(IsDescartes)
				innerRow = s.EqualValue(outerValue.String())
			}
		}

		if proto.CompareValue(outerValue, innerValue) > 0 {
			s.CleanBeforeRow()
			// if outer row equal last row, do descartes match
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}

			if s.isDescartes(outerValue.String()) {
				s.setDescartesFlag(IsDescartes)
				innerRow = s.EqualValue(outerValue.String())
			} else {
				s.setDescartesFlag(NotDescartes)
				innerRow, err = s.getInnerRow(outerRow)
				if err != nil {
					return nil, err
				}
			}
		}
	}
}

/*func (s *SortMergeJoin) innerJoin() (proto.Row, error) {
	var (
		err                error
		outerRow, innerRow proto.Row
	)

	if s.BeforeRow() != nil {
		outerRow = s.BeforeRow()
	} else {
		outerRow, err = s.getOuterRow()
		if err != nil {
			return nil, err
		}
	}

	innerRow, err = s.getInnerRow(outerRow)
	if err != nil {
		return nil, err
	}

	var outerValue, innerValue proto.Value

	for {
		if outerRow == nil || innerRow == nil {
			return nil, io.EOF
		}

		outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		innerValue, err = innerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		if proto.CompareValue(outerValue, innerValue) == 0 {
			// restore last value
			// example : 1,2,2 => 2,2,3
			// first left 2 match right first 2
			// next still need left 2  match next second 2
			s.SetBeforeRow(outerRow)
			if s.DescartesFlag() {
				index := s.EqualIndex(outerValue.String())
				if index == 0 {
					nextOuterRow, err := s.getOuterRow()
					if err != nil {
						return nil, err
					}
					s.SetBeforeRow(nextOuterRow)
				}
			}

			if !s.DescartesFlag() {
				s.SetEqualValue(cast.ToString(outerValue), innerRow)
			}

			return s.resGenerate(outerRow, innerRow), nil
		}

		if proto.CompareValue(outerValue, innerValue) < 0 {
			s.CleanBeforeRow()
			outerRow, err = s.getOuterRow()
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}

			// if outer row equal last row, do descartes match
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			s.setDescartesFlag(NotDescartes)
			if s.isDescartes(cast.ToString(outerValue)) {
				s.setDescartesFlag(IsDescartes)
				innerRow = s.EqualValue(cast.ToString(outerValue))
			}
		}

		if proto.CompareValue(outerValue, innerValue) > 0 {
			s.CleanBeforeRow()
			// if outer row equal last row, do descartes match
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}

			if s.isDescartes(cast.ToString(outerValue)) {
				s.setDescartesFlag(IsDescartes)
				innerRow = s.EqualValue(cast.ToString(outerValue))
			} else {
				s.setDescartesFlag(NotDescartes)
				innerRow, err = s.getInnerRow(outerRow)
				if err != nil {
					return nil, err
				}
			}
		}
	}
}*/

func (s *SortMergeJoin) leftJoin(outerRow proto.Row, innerRow proto.Row) (proto.Row, error) {
	var (
		err                    error
		outerValue, innerValue proto.Value
	)

	for {
		if outerRow == nil || innerRow == nil {
			return nil, io.EOF
		}

		outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		innerValue, err = innerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		if proto.CompareValue(outerValue, innerValue) == 0 {
			// restore last value
			// example : 1,2,2 => 2,2,3
			// first left 2 match right first 2
			// next still need left 2  match next second 2
			s.SetBeforeRow(outerRow)
			if s.DescartesFlag() {
				index := s.EqualIndex(outerValue.String())
				if index == 0 {
					nextOuterRow, err := s.getOuterRow()
					if err != nil {
						return nil, err
					}
					s.SetBeforeRow(nextOuterRow)
				}
			}

			if !s.DescartesFlag() {
				s.SetEqualValue(cast.ToString(outerValue), innerRow)
			}

			return s.resGenerate(outerRow, innerRow), nil
		}

		if proto.CompareValue(outerValue, innerValue) < 0 {
			s.CleanBeforeRow()
			outerRow, err = s.getOuterRow()
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}

			// if outer row equal last row, do descartes match
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			s.setDescartesFlag(NotDescartes)
			if s.isDescartes(outerValue.String()) {
				s.setDescartesFlag(IsDescartes)
				innerRow = s.EqualValue(outerValue.String())
			} else {
				return s.resGenerate(outerRow, nil), nil
			}
		}

		if proto.CompareValue(outerValue, innerValue) > 0 {
			s.CleanBeforeRow()
			// if outer row equal last row, do descartes match
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}

			if s.isDescartes(outerValue.String()) {
				s.setDescartesFlag(IsDescartes)
				innerRow = s.EqualValue(outerValue.String())
			} else {
				s.setDescartesFlag(NotDescartes)
				return s.resGenerate(outerRow, nil), nil
				/*innerRow, err = s.getInnerRow(outerRow)
				if err != nil {
					return nil, err
				}*/
			}
		}
	}
}

// leftJoin
/*func (s *SortMergeJoin) leftJoin(outerRow proto.Row, innerRow proto.Row) (proto.Row, error) {
	var (
		err                    error
		outerValue, innerValue proto.Value
	)

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
			// restore last value
			// example : 1,2,2 => 2,2,3
			// first left 2 match right first 2
			// next still need left 2  match next second 2
			s.SetBeforeRow(outerRow)
			return s.resGenerate(outerRow, innerRow), nil
		}

		if strings.Compare(outerValue.String(), innerValue.String()) < 0 {
			s.SetBeforeRow(innerRow)
			// return left row + null
			return s.resGenerate(outerRow, nil), nil
		}

		if strings.Compare(outerValue.String(), innerValue.String()) > 0 {
			s.SetBeforeRow(outerRow)
			// return right row + null
			return s.resGenerate(outerRow, nil), nil
		}
	}
}*/

func (s *SortMergeJoin) rightJoin() (proto.Row, error) {
	var (
		err                error
		outerRow, innerRow proto.Row
	)

	// all data is order
	if s.BeforeRow() != nil {
		outerRow = s.BeforeRow()
	} else {
		outerRow, err = s.getOuterRow()
		if err != nil {
			return nil, err
		}
	}

	innerRow, err = s.getInnerRow(outerRow)
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
			// restore last value
			// example : 1,2,2 => 2,2,3
			// first left 2 match right first 2
			// next still need left 2  match next second 2
			s.SetBeforeRow(outerRow)
			return s.resGenerate(outerRow, innerRow), nil
		}

		if strings.Compare(outerValue.String(), innerValue.String()) < 0 {
			s.SetBeforeRow(innerRow)
			// return left row + null
			return s.resGenerate(outerRow, nil), nil
		}

		if strings.Compare(outerValue.String(), innerValue.String()) > 0 {
			s.SetBeforeRow(outerRow)
			// return right row + null
			return s.resGenerate(innerRow, nil), nil
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

func (s *SortMergeJoin) getInnerRow(outerRow proto.Row) (proto.Row, error) {
	outerValue, err := outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
	if err != nil {
		return nil, err
	}

	if s.DescartesFlag() {
		return s.EqualValue(outerValue.String()), nil
	}

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
	var (
		leftValue  []proto.Value
		rightValue []proto.Value
		res        []proto.Value
	)

	if leftRow == nil && rightRow == nil {
		return nil
	}

	leftFields, _ := s.outer.Fields()
	rightFields, _ := s.inner.Fields()

	leftValue = make([]proto.Value, len(leftFields))
	rightValue = make([]proto.Value, len(rightFields))

	if leftRow == nil {
		if err := rightRow.(proto.KeyedRow).Scan(rightValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
	}

	if rightRow == nil {
		if err := leftRow.(proto.KeyedRow).Scan(leftValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
	}

	if leftRow != nil && rightRow != nil {
		if err := leftRow.(proto.KeyedRow).Scan(leftValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
		if err := rightRow.(proto.KeyedRow).Scan(rightValue); err != nil {
			log.Infof("left row scan error, err: %+v", err)
			return nil
		}
	}

	res = append(res, leftValue...)
	res = append(res, rightValue...)

	fields, _ := s.Fields()

	return rows.NewBinaryVirtualRow(fields, res)
}

/*func (s *SortMergeJoin) getDescartesRow() proto.Row {

}*/
