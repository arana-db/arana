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

package ast

import (
	"database/sql"
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

type baseShow struct {
	filter interface{} // ExpressionNode or string
}

func (bs *baseShow) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch val := bs.filter.(type) {
	case string:
		sb.WriteString(" IN ")
		sb.WriteByte('`')
		sb.WriteString(val)
		sb.WriteByte('`')
		return nil
	case PredicateNode:
		return val.Restore(flag, sb, nil)
	case ExpressionNode:
		sb.WriteString(" WHERE ")
		return val.Restore(flag, sb, args)
	default:
		return nil
	}
}

func (bs *baseShow) Like() (string, bool) {
	v, ok := bs.filter.(string)
	return v, ok
}
func (bs *baseShow) Where() (ExpressionNode, bool) {
	v, ok := bs.filter.(ExpressionNode)
	return v, ok
}

func (bs *baseShow) CntParams() int {
	return 0
}

func (bs *baseShow) Mode() SQLType {
	return Squery
}

type ShowDatabases struct {
	*baseShow
}

func (s ShowDatabases) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW DATABASES")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s ShowDatabases) Validate() error {
	return nil
}

type ShowTables struct {
	*baseShow
}

func (s ShowTables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW TABLES")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s ShowTables) Validate() error {
	return nil
}

type ShowOpenTables struct {
	*baseShow
}

func (s ShowOpenTables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW OPEN TABLES")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s ShowOpenTables) Validate() error {
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

func (s *ShowCreate) ResetTable(table string) *ShowCreate {
	ret := new(ShowCreate)
	*ret = *s
	ret.tgt = table
	return ret
}

func (s *ShowCreate) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW CREATE ")
	sb.WriteString(s.typ.String())
	sb.WriteByte(' ')
	WriteID(sb, s.tgt)
	return nil
}

func (s *ShowCreate) Validate() error {
	return nil
}

func (s *ShowCreate) Type() ShowCreateType {
	return s.typ
}

func (s *ShowCreate) Target() string {
	return s.tgt
}

func (s *ShowCreate) CntParams() int {
	return 0
}

func (s *ShowCreate) Mode() SQLType {
	return Squery
}

type ShowIndex struct {
	TableName TableName
	where     ExpressionNode
}

func (s *ShowIndex) Validate() error {
	return nil
}

func (s *ShowIndex) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW INDEXES FROM ")

	_ = s.TableName.Restore(flag, sb, args)

	if where, ok := s.Where(); ok {
		sb.WriteString(" WHERE ")
		if err := where.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (s *ShowIndex) Where() (ExpressionNode, bool) {
	if s.where != nil {
		return s.where, true
	}
	return nil, false
}

func (s *ShowIndex) CntParams() int {
	if s.where == nil {
		return 0
	}
	return s.where.CntParams()
}

func (s *ShowIndex) Mode() SQLType {
	return Squery
}

type showColumnsFlag uint8

const (
	scFlagFull showColumnsFlag = 0x01 << iota
	scFlagExtended
	scFlagFields
	scFlagIn
)

type ShowColumns struct {
	flag      showColumnsFlag
	TableName TableName
	like      sql.NullString
	Column    string
}

func (sh *ShowColumns) IsFull() bool {
	return sh.flag&scFlagFull != 0
}

func (sh *ShowColumns) IsExtended() bool {
	return sh.flag&scFlagExtended != 0
}

func (sh *ShowColumns) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW ")

	if sh.IsExtended() {
		sb.WriteString("EXTENDED ")
	}
	if sh.IsFull() {
		sb.WriteString("FULL ")
	}

	sb.WriteString("COLUMNS FROM ")
	if err := sh.TableName.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if sh.like.Valid {
		sb.WriteString(" LIKE ")

		sb.WriteByte('\'')
		sb.WriteString(sh.like.String)
		sb.WriteByte('\'')
	}

	return nil
}

func (sh *ShowColumns) Like() (string, bool) {
	if sh.like.Valid {
		return sh.like.String, true
	}
	return "", false
}

func (sh *ShowColumns) Validate() error {
	return nil
}

func (sh *ShowColumns) Table() TableName {
	return sh.TableName
}

func (sh *ShowColumns) CntParams() int {
	return 0
}

func (sh *ShowColumns) Mode() SQLType {
	return Squery
}

func (sh *ShowColumns) Full() bool {
	return sh.flag&scFlagFull != 0
}

func (sh *ShowColumns) Extended() bool {
	return sh.flag&scFlagExtended != 0
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

type ShowVariables struct {
	flag showColumnsFlag
	like sql.NullString
}

func (s *ShowVariables) Validate() error {
	return nil
}

func (s *ShowVariables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW VARIABLES ")

	if s.like.Valid {
		sb.WriteString(" LIKE ")

		sb.WriteByte('\'')
		sb.WriteString(s.like.String)
		sb.WriteByte('\'')
	}

	return nil
}

func (s *ShowVariables) Like() (string, bool) {
	if s.like.Valid {
		return s.like.String, true
	}
	return "", false
}

func (s *ShowVariables) CntParams() int {
	return 0
}

func (s *ShowVariables) Mode() SQLType {
	return Squery
}
