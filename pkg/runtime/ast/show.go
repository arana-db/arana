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

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/misc"
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
	_ Statement = (*ShowReplicaStatus)(nil)
	_ Statement = (*ShowDatabaseRule)(nil)
)

var TruePredicate = func(row proto.Row) bool { return true }

type FromTable string

func (f FromTable) String() string {
	return string(f)
}

type FromDatabase string

func (f FromDatabase) String() string {
	return string(f)
}

type BaseShow struct {
	filter interface{} // ExpressionNode or string
}

func NewBaseShow(filter string) BaseShow {
	return BaseShow{filter: filter}
}

func (bs *BaseShow) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch val := bs.filter.(type) {
	case string:
		sb.WriteString(" IN ")
		WriteID(sb, val)
		return nil
	case FromTable:
		sb.WriteString(val.String())
		return nil
	case FromDatabase:
		sb.WriteString(" FROM ")
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

func (bs *BaseShow) Like() (string, bool) {
	v, ok := bs.filter.(string)
	return v, ok
}

func (bs *BaseShow) Where() (ExpressionNode, bool) {
	v, ok := bs.filter.(ExpressionNode)
	return v, ok
}

func (bs *BaseShow) Filter() func(proto.Row) bool {
	return TruePredicate
}

// BaseShowWithSingleColumn for `show databases` and `show tables` clause which only have one column.
// Get result and do filter locally
type BaseShowWithSingleColumn struct {
	*BaseShow
	like sql.NullString
}

func (bs *BaseShowWithSingleColumn) Like() (string, bool) {
	if bs.like.Valid {
		return bs.like.String, true
	}
	v, ok := bs.filter.(string)
	return v, ok
}

func (bs *BaseShowWithSingleColumn) Filter() func(proto.Row) bool {
	if pattern, ok := bs.Like(); ok {
		liker := misc.NewLiker(pattern)
		return func(row proto.Row) bool {
			dest := make([]proto.Value, 1)
			if row.Scan(dest) != nil {
				return false
			}
			return liker.Like(dest[0].String())
		}
	}

	// TODO make it cleaner
	if whereFilter, ok := bs.Where(); ok {
		target := whereFilter.(*PredicateExpressionNode).
			P.(*BinaryComparisonPredicateNode).
			Right.(*AtomPredicateNode).
			A.(*ConstantExpressionAtom).
			Inner.(string)
		return func(row proto.Row) bool {
			dest := make([]proto.Value, 1)
			if row.Scan(dest) != nil {
				return false
			}
			return target == dest[0].String()
		}
	}

	return TruePredicate
}

type ShowDatabases struct {
	*BaseShowWithSingleColumn
}

func (s ShowDatabases) Mode() SQLType {
	return SQLTypeShowDatabases
}

func (s ShowDatabases) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW DATABASES")
	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowCollation struct {
	*BaseShow
}

func (s ShowCollation) Mode() SQLType {
	return SQLTypeShowCollation
}

func (s ShowCollation) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW COLLATION")
	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowTables struct {
	*BaseShowWithSingleColumn
}

func (st *ShowTables) Mode() SQLType {
	return SQLTypeShowTables
}

func (st *ShowTables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW TABLES")
	if err := st.BaseShowWithSingleColumn.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowTopology struct {
	*BaseShow
}

func (s ShowTopology) Mode() SQLType {
	return SQLTypeShowTopology
}

func (s ShowTopology) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	return s.BaseShow.Restore(flag, sb, args)
}

type ShowOpenTables struct {
	*BaseShow
}

func (s ShowOpenTables) Mode() SQLType {
	return SQLTypeShowOpenTables
}

func (s ShowOpenTables) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW OPEN TABLES")
	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
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

func (s *ShowVariables) Mode() SQLType {
	return SQLTypeShowVariables
}

type ShowStatus struct {
	*BaseShow
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

	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (s *ShowStatus) Mode() SQLType {
	return SQLTypeShowStatus
}

type ShowTableStatus struct {
	*BaseShow
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
	*BaseShow
	Limit *LimitNode
}

func (s *ShowWarnings) Validate() error {
	return nil
}

func (s *ShowWarnings) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	// Todo implements 1: SHOW WARNINGS [LIMIT [offset,] row_count],  2: SHOW COUNT(*) WARNINGS
	sb.WriteString("SHOW WARNINGS")

	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	if s.Limit != nil {
		sb.WriteString(" LIMIT ")
		if err := s.Limit.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (s *ShowWarnings) Mode() SQLType {
	return SQLTypeShowWarnings
}

type ShowCharset struct {
	*BaseShow
}

func (s *ShowCharset) Mode() SQLType {
	return SQLTypeShowCharacterSet
}

func (s *ShowCharset) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW CHARACTER SET")

	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

type ShowReplicas struct {
	*BaseShow
}

func (s ShowReplicas) Mode() SQLType {
	return SQLTypeShowReplicas
}

func (s ShowReplicas) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW REPLICAS ")

	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

type ShowMasterStatus struct {
	*BaseShow
}

func (s *ShowMasterStatus) Mode() SQLType {
	return SQLTypeShowMasterStatus
}

func (s *ShowMasterStatus) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW MASTER STATUS")

	return s.BaseShow.Restore(flag, sb, args)
}

type ShowProcessList struct {
	*BaseShow
}

func (s *ShowProcessList) Mode() SQLType {
	return SQLTypeShowProcessList
}

func (s *ShowProcessList) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW PROCESSLIST")

	return s.BaseShow.Restore(flag, sb, args)
}

type ShowReplicaStatus struct {
	*BaseShow
}

func (s *ShowReplicaStatus) Mode() SQLType {
	return SQLTypeShowReplicaStatus
}

func (s *ShowReplicaStatus) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW REPLICA STATUS")
	return s.BaseShow.Restore(flag, sb, args)
}

type ShowNodes struct {
	Tenant string
}

func (s *ShowNodes) Mode() SQLType {
	return SQLTypeShowNodes
}

func (s *ShowNodes) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW NODES FROM ")

	if len(s.Tenant) > 0 {
		WriteID(sb, s.Tenant)
	}

	return nil
}

type ShowUsers struct {
	Tenant string
}

func (s *ShowUsers) Mode() SQLType {
	return SQLTypeShowUsers
}

func (s *ShowUsers) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW USERS FROM ")

	if len(s.Tenant) > 0 {
		WriteID(sb, s.Tenant)
	}

	return nil
}

type ShowShardingTable struct {
	*BaseShow
}

func (s *ShowShardingTable) Mode() SQLType {
	return SQLTypeShowShardingTable
}

func (s *ShowShardingTable) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	val, ok := s.BaseShow.filter.(string)
	if !ok {
		return errors.New("show sharding table database type error")
	}
	sb.WriteString(val)
	return nil
}

type ShowCreateSequence struct {
	Tenant string
}

func (s *ShowCreateSequence) Mode() SQLType {
	return SQLTypeShowCreateSequence
}

func (s *ShowCreateSequence) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW CREATE SEQUENCE ")

	if len(s.Tenant) > 0 {
		WriteID(sb, s.Tenant)
	}
	return nil
}

type ShowDatabaseRule struct {
	*BaseShow
	Database  string
	TableName string
}

func (s *ShowDatabaseRule) Mode() SQLType {
	return SQLTypeShowDatabaseRules
}

func (s *ShowDatabaseRule) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW DATABASE RULES ")
	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type ShowTableRule struct {
	*BaseShow
	Database  string
	TableName string
}

func (s *ShowTableRule) Mode() SQLType {
	return SQLTypeShowTableRules
}

func (s *ShowTableRule) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SHOW TABLE RULES ")
	if err := s.BaseShow.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
