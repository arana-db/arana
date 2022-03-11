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

package ast

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

var (
	_ Statement = (*ShowTables)(nil)
	_ Statement = (*ShowCreate)(nil)
	_ Statement = (*ShowDatabases)(nil)
	_ Statement = (*ShowColumns)(nil)
	_ Statement = (*ShowIndex)(nil)
)

var (
	_ Restorer = (*ShowTables)(nil)
	_ Restorer = (*ShowCreate)(nil)
	_ Restorer = (*ShowDatabases)(nil)
	_ Restorer = (*ShowColumns)(nil)
	_ Restorer = (*ShowIndex)(nil)
)

type baseShow struct {
	filter interface{} // 1. string -> like, 2. expr -> where
}

func (bs *baseShow) Like() (string, bool) {
	v, ok := bs.filter.(string)
	return v, ok
}
func (bs *baseShow) Where() (ExpressionNode, bool) {
	v, ok := bs.filter.(ExpressionNode)
	return v, ok
}

func (bs *baseShow) GetSQLType() SQLType {
	return Squery
}

type ShowDatabases struct {
	*baseShow
}

func (s ShowDatabases) Restore(sb *strings.Builder, args *[]int) error {
	//TODO implement me
	panic("implement me")
}

func (s ShowDatabases) Validate() error {
	return nil
}

type ShowTables struct {
	*baseShow
}

func (s ShowTables) Restore(sb *strings.Builder, args *[]int) error {
	//TODO implement me
	panic("implement me")
}

func (s ShowTables) Validate() error {
	return nil
}

const (
	_ ShowCreateType = iota
	ShowCreateTypeTable
	ShowCreateTypeEvent
	ShowCreateTypeFunc
	ShowCreateTypeProc
	ShowCreateTypeTrigger
	ShowCreateTypeView
)

type ShowCreateType uint8

func (s ShowCreateType) String() string {
	switch s {
	case ShowCreateTypeEvent:
		return "EVENT"
	case ShowCreateTypeTable:
		return "TABLE"
	case ShowCreateTypeFunc:
		return "FUNCTION"
	case ShowCreateTypeProc:
		return "PROCEDURE"
	case ShowCreateTypeTrigger:
		return "TRIGGER"
	case ShowCreateTypeView:
		return "VIEW"
	default:
		return ""
	}
}

type ShowCreate struct {
	typ ShowCreateType
	tgt string
}

func (s *ShowCreate) Restore(sb *strings.Builder, args *[]int) error {
	//TODO implement me
	panic("implement me")
}

func (s *ShowCreate) Validate() error {
	return nil
}

func (s *ShowCreate) GetShowCreateType() ShowCreateType {
	return s.typ
}

func (s *ShowCreate) Target() string {
	return s.tgt
}

func (s *ShowCreate) GetSQLType() SQLType {
	return Squery
}

const (
	_ ShowIndexType = iota
	ShowIndexEnumIndex
	ShowIndexEnumIndexes
	ShowIndexEnumKeys
)

type ShowIndexType uint8

func (s ShowIndexType) String() string {
	switch s {
	case ShowIndexEnumIndex:
		return "INDEX"
	case ShowIndexEnumIndexes:
		return "INDEXES"
	case ShowIndexEnumKeys:
		return "KEYS"
	default:
		return ""
	}
}

type ShowIndex struct {
	typ       ShowIndexType
	tableName TableName
	where     ExpressionNode
}

func (s *ShowIndex) Validate() error {
	return nil
}

func (s *ShowIndex) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW")
	sb.WriteByte(' ')
	sb.WriteString(s.typ.String())
	sb.WriteString(" FROM ")

	sb.WriteString(s.tableName.String())

	if where, ok := s.Where(); ok {
		sb.WriteString(" WHERE ")
		if err := where.Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (s *ShowIndex) GetType() ShowIndexType {
	return s.typ
}

func (s *ShowIndex) TableName() TableName {
	return s.tableName
}

func (s *ShowIndex) Where() (ExpressionNode, bool) {
	if s.where != nil {
		return s.where, true
	}
	return nil, false
}

func (s *ShowIndex) GetSQLType() SQLType {
	return Squery
}

type showColumnsFlag uint8

const (
	scFlagFull showColumnsFlag = 0x01 << iota
	scFlagFields
	scFlagIn
)

type ShowColumns struct {
	flag      showColumnsFlag
	tableName TableName
}

func (sh *ShowColumns) Restore(sb *strings.Builder, args *[]int) error {
	//TODO implement me
	panic("implement me")
}

func (sh *ShowColumns) Validate() error {
	return nil
}

func (sh *ShowColumns) Table() TableName {
	return sh.tableName
}

func (sh *ShowColumns) GetSQLType() SQLType {
	return Squery
}

func (sh *ShowColumns) Full() bool {
	return sh.flag&scFlagFull != 0
}

func (sh *ShowColumns) ColumnsFormat() string {
	if sh.flag&scFlagFields != 0 {
		return "FIELDS"
	}
	return "COLUMNS"
}

func (sh *ShowColumns) TableFormat() string {
	if sh.flag&scFlagIn != 0 {
		return "IN"
	}
	return "FROM"
}
