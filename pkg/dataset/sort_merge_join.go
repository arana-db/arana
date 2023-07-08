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
	"sync"
)

import (
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"go.uber.org/atomic"
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
	joinType     ast.JoinType
	outer        proto.Dataset
	inner        proto.Dataset
	lastRow      proto.Row
	lastInnerRow proto.Row
	nextOuterRow proto.Row
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

	if joinType == ast.RightJoin {
		outer, inner = inner, outer
	}

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

func (s *SortMergeJoin) SetLastRow(v proto.Row) {
	if s != nil {
		s.lastRow = v
	}
}

func (s *SortMergeJoin) LastRow() proto.Row {
	if s != nil {
		return s.lastRow
	}

	return nil
}

func (s *SortMergeJoin) ResetLastRow() {
	if s != nil {
		s.lastRow = nil
	}
}

func (s *SortMergeJoin) SetLastInnerRow(v proto.Row) {
	if s != nil {
		s.lastInnerRow = v
	}
}

func (s *SortMergeJoin) LastInnerRow() proto.Row {
	if s != nil {
		return s.lastInnerRow
	}

	return nil
}

func (s *SortMergeJoin) ResetLastInnerRow() {
	if s != nil {
		s.lastInnerRow = nil
	}
}

func (s *SortMergeJoin) SetNextOuterRow(v proto.Row) {
	if s != nil {
		s.nextOuterRow = v
	}
}

func (s *SortMergeJoin) NextOuterRow() proto.Row {
	if s != nil {
		return s.nextOuterRow
	}

	return nil
}

func (s *SortMergeJoin) ResetNextOuterRow() {
	if s != nil {
		s.nextOuterRow = nil
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

	if s.LastRow() != nil {
		outerRow = s.LastRow()
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
		return s.rightJoin(outerRow, innerRow)
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
			if res, err := s.equalCompare(outerRow, innerRow, outerValue); err != nil {
				return nil, err
			} else {
				return res, nil
			}
		}

		if proto.CompareValue(outerValue, innerValue) < 0 {
			s.ResetLastRow()
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
			outerRow, innerRow, err = s.greaterCompare(outerRow)
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}
		}
	}
}

// leftJoin
func (s *SortMergeJoin) leftJoin(outerRow proto.Row, innerRow proto.Row) (proto.Row, error) {
	var (
		err                    error
		outerValue, innerValue proto.Value
	)

	for {
		if outerRow == nil {
			return nil, io.EOF
		}

		if innerRow == nil {
			s.ResetLastRow()
			outerValue, err = outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
			if err != nil {
				return nil, err
			}

			if s.isDescartes(outerValue.String()) {
				outerRow, err = s.getOuterRow()
				if err != nil {
					return nil, err
				}
				continue
			}

			return s.resGenerate(outerRow, nil), nil
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
			if res, err := s.equalCompare(outerRow, innerRow, outerValue); err != nil {
				return nil, err
			} else {
				return res, nil
			}
		}

		if proto.CompareValue(outerValue, innerValue) < 0 {
			s.ResetLastRow()
			nextOuterRow, err := s.getOuterRow()
			if err != nil {
				return nil, err
			}

			if nextOuterRow != nil {
				// if outer row equal last row, do descartes match
				nextOuterValue, err := nextOuterRow.(proto.KeyedRow).Get(s.joinColumn.Column())
				if err != nil {
					return nil, err
				}

				s.setDescartesFlag(NotDescartes)
				// record last inner row
				s.SetLastInnerRow(innerRow)
				if s.isDescartes(nextOuterValue.String()) {
					s.setDescartesFlag(IsDescartes)
					innerRow = s.EqualValue(nextOuterValue.String())
					outerRow = nextOuterRow
				} else {
					if s.isDescartes(outerValue.String()) {
						if proto.CompareValue(nextOuterValue, innerValue) == 0 {
							s.ResetLastInnerRow()
							outerRow = nextOuterRow
						} else {
							return s.resGenerate(nextOuterRow, nil), nil
						}
					} else {
						s.SetNextOuterRow(nextOuterRow)
						return s.resGenerate(outerRow, nil), nil
					}
				}
			} else {
				if !s.isDescartes(outerValue.String()) {
					return s.resGenerate(outerRow, nil), nil
				}
				return nil, nil
			}
		}

		if proto.CompareValue(outerValue, innerValue) > 0 {
			outerRow, innerRow, err = s.greaterCompare(outerRow)
			if err != nil {
				return nil, err
			}

			if outerRow == nil {
				return nil, io.EOF
			}
		}
	}
}

// rightJoin
func (s *SortMergeJoin) rightJoin(outerRow proto.Row, innerRow proto.Row) (proto.Row, error) {
	return s.leftJoin(outerRow, innerRow)
}

func (s *SortMergeJoin) getOuterRow() (proto.Row, error) {
	nextOuterRow := s.NextOuterRow()
	if nextOuterRow != nil {
		s.ResetNextOuterRow()
		return nextOuterRow, nil
	}

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
	if outerRow != nil {
		outerValue, err := outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
		if err != nil {
			return nil, err
		}

		if s.DescartesFlag() {
			innerRow := s.EqualValue(outerValue.String())
			if innerRow != nil {
				return innerRow, nil
			}
		}
	}

	lastInnerRow := s.LastInnerRow()
	if lastInnerRow != nil {
		s.ResetLastInnerRow()
		return lastInnerRow, nil
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

func (s *SortMergeJoin) equalCompare(outerRow proto.Row, innerRow proto.Row, outerValue proto.Value) (proto.Row, error) {
	if err := s.updateLastRow(outerRow, outerValue); err != nil {
		return nil, err
	}

	if !s.DescartesFlag() {
		s.SetEqualValue(cast.ToString(outerValue), innerRow)
	}

	return s.resGenerate(outerRow, innerRow), nil
}

func (s *SortMergeJoin) updateLastRow(outerRow proto.Row, outerValue proto.Value) error {
	s.SetLastRow(outerRow)
	if s.DescartesFlag() {
		index := s.EqualIndex(outerValue.String())
		if index == 0 {
			nextOuterRow, err := s.getOuterRow()
			if err != nil {
				return err
			}
			s.SetLastRow(nextOuterRow)
		}
	}

	return nil
}

func (s *SortMergeJoin) greaterCompare(outerRow proto.Row) (proto.Row, proto.Row, error) {
	s.ResetLastRow()
	// if outer row equal last row, do descartes match
	outerValue, err := outerRow.(proto.KeyedRow).Get(s.joinColumn.Column())
	if err != nil {
		return nil, nil, err
	}

	if outerRow == nil {
		return nil, nil, nil
	}

	var innerRow proto.Row
	if s.isDescartes(outerValue.String()) {
		s.setDescartesFlag(IsDescartes)
		innerRow = s.EqualValue(outerValue.String())
	} else {
		s.setDescartesFlag(NotDescartes)
		innerRow, err = s.getInnerRow(outerRow)
		if err != nil {
			return nil, nil, err
		}
	}

	return outerRow, innerRow, nil
}
