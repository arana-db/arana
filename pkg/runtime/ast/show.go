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
	_ Statement = (*ShowOpenTables)(nil)
	_ Statement = (*ShowCreate)(nil)
	_ Statement = (*ShowDatabases)(nil)
	_ Statement = (*ShowColumns)(nil)
	_ Statement = (*ShowIndex)(nil)
	_ Statement = (*ShowTopology)(nil)
	_ Statement = (*ShowTableStatus)(nil)
	_ Statement = (*ShowWarnings)(nil)
	_ Statement = (*ShowMasterStatus)(nil)
)

type FromTable string

func (f FromTable) String() string {
	return string(f)
}

type baseShow struct {
	filter interface{} // ExpressionNode or string
}

func (bs *baseShow) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch val := bs.filter.(type) {
	case string:
		sb.WriteString(" IN ")
		WriteID(sb, val)
		return nil
	case FromTable:
		sb.WriteString(val.String())
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

type ShowDatabases struct {
	*baseShow
}

func (s ShowDatabases) Mode() SQLType {
	return SQLTypeShowDatabases
}

func (s ShowDatabases) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW DATABASES")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowCollation struct {
	*baseShow
}

func (s ShowCollation) Mode() SQLType {
	return SQLTypeShowCollation
}

func (s ShowCollation) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW COLLATION")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowTables struct {
	*baseShow
}

func (s ShowTables) Mode() SQLType {
	return SQLTypeShowTables
}

func (s ShowTables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW TABLES")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowTopology struct {
	*baseShow
}

func (s ShowTopology) Mode() SQLType {
	return SQLTypeShowTopology
}

func (s ShowTopology) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	return s.baseShow.Restore(flag, sb, args)
}

type ShowOpenTables struct {
	*baseShow
}

func (s ShowOpenTables) Mode() SQLType {
	return SQLTypeShowOpenTables
}

func (s ShowOpenTables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW OPEN TABLES")
	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
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

var _showCreateTypes = [...]string{
	ShowCreateTypeEvent:   "EVENT",
	ShowCreateTypeTable:   "TABLE",
	ShowCreateTypeFunc:    "FUNCTION",
	ShowCreateTypeProc:    "PROCEDURE",
	ShowCreateTypeTrigger: "TRIGGER",
	ShowCreateTypeView:    "VIEW",
}

type ShowCreateType uint8

func (s ShowCreateType) String() string {
	return _showCreateTypes[s]
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
	return SQLTypeShowCreate
}

type ShowIndex struct {
	TableName TableName
	where     ExpressionNode
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
	return SQLTypeShowIndex
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

func (sh *ShowColumns) Table() TableName {
	return sh.TableName
}

func (sh *ShowColumns) CntParams() int {
	return 0
}

func (sh *ShowColumns) Mode() SQLType {
	return SQLTypeShowColumns
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
	return SQLTypeShowVariables
}

type ShowStatus struct {
	*baseShow
	flag   showColumnsFlag
	global bool
}

func (s *ShowStatus) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW ")

	if s.global {
		sb.WriteString(" GLOBAL ")
	} else {
		sb.WriteString(" SESSION ")
	}
	sb.WriteString(" STATUS ")

	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *ShowStatus) Mode() SQLType {
	return SQLTypeShowStatus
}

type ShowTableStatus struct {
	*baseShow
	Database string
}

func (s *ShowTableStatus) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW TABLE STATUS")

	if len(s.Database) > 0 {
		sb.WriteString(" FROM ")
		WriteID(sb, s.Database)
	}

	if where, ok := s.Where(); ok {
		sb.WriteString(" WHERE ")
		if err := where.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if like, ok := s.Like(); ok {
		sb.WriteString(" LIKE ")
		WriteString(sb, like)
	}

	return nil
}

func (s *ShowTableStatus) Mode() SQLType {
	return SQLTypeShowTableStatus
}

type ShowWarnings struct {
	*baseShow
	Limit *LimitNode
}

func (s *ShowWarnings) Validate() error {
	return nil
}

func (s *ShowWarnings) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	// Todo implements 1: SHOW WARNINGS [LIMIT [offset,] row_count],  2: SHOW COUNT(*) WARNINGS
	sb.WriteString("SHOW WARNINGS")

	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *ShowWarnings) Mode() SQLType {
	return SQLTypeShowWarnings
}

type ShowCharset struct {
	*baseShow
}

func (s *ShowCharset) Mode() SQLType {
	return SQLTypeShowCharacterSet
}

func (s *ShowCharset) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW CHARACTER SET")

	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

type ShowMasterStatus struct {
	*baseShow
}

func (s *ShowMasterStatus) Mode() SQLType {
	return SQLTypeShowMasterStatus
}

func (s *ShowMasterStatus) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW MASTER STATUS")

	return s.baseShow.Restore(flag, sb, args)
}

type ShowReplicas struct {
	*baseShow
}

func (s ShowReplicas) Mode() SQLType {
	return SQLTypeShowReplicas
}

func (s ShowReplicas) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW REPLICAS")

	if err := s.baseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	
	return nil
}
