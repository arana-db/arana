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
	_ inTablesChecker = (OrderByNode)(nil)
	_ inTablesChecker = (*OrderByItem)(nil)
	_ inTablesChecker = (*GroupByNode)(nil)
	_ inTablesChecker = (*GroupByItem)(nil)
)

type Null struct{}

func (n Null) String() string {
	return "NULL"
}

type TableName []string

func (t TableName) String() string {
	var sb strings.Builder
	sb.WriteByte('`')
	sb.WriteString(t[0])
	sb.WriteByte('`')

	for i := 1; i < len(t); i++ {
		sb.WriteByte('.')
		sb.WriteByte('`')
		sb.WriteString(t[i])
		sb.WriteByte('`')
	}
	return sb.String()
}

func (t TableName) Prefix() (prefix string) {
	if len(t) > 1 {
		prefix = t[0]
	}
	return
}

func (t TableName) Suffix() string {
	return t[len(t)-1]
}

type TableSourceNode struct {
	source interface{} // TableName or Statement
	alias  string
}

func NewTableSourceNodeTable(tableName TableName, alias string) *TableSourceNode {
	return &TableSourceNode{source: tableName, alias: alias}
}

func (t *TableSourceNode) Alias() string {
	return t.alias
}

func (t *TableSourceNode) TableName() TableName {
	tn, ok := t.source.(TableName)
	if ok {
		return tn
	}
	return nil
}

func (t *TableSourceNode) SubQuery() Statement {
	stmt, ok := t.source.(Statement)
	if ok {
		return stmt
	}
	return nil
}

type OrderByItem struct {
	Alias string
	Expr  ExpressionAtom
	Desc  bool
}

func (o OrderByItem) InTables(tables map[string]struct{}) error {
	if o.Expr != nil {
		if err := o.Expr.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (o OrderByItem) String() string {
	var sb strings.Builder
	if err := o.Expr.Restore(&sb, nil); err != nil {
		panic(err.Error())
	}
	if o.Desc {
		sb.WriteString(" DESC")
	}
	return sb.String()
}

const (
	flagGroupByHasOrder uint8 = 0x01 << iota
	flagGroupByOrderDesc
)

type GroupByItem struct {
	expr ExpressionNode
	flag uint8
}

func (gb *GroupByItem) InTables(tables map[string]struct{}) error {
	return gb.expr.InTables(tables)
}

func (gb *GroupByItem) Expr() ExpressionNode {
	return gb.expr
}

func (gb *GroupByItem) HasOrder() bool {
	return gb.flag&flagGroupByHasOrder != 0
}

func (gb *GroupByItem) IsOrderDesc() bool {
	return gb.flag&flagGroupByOrderDesc != 0
}

type OrderByNode []*OrderByItem

func (o OrderByNode) InTables(tables map[string]struct{}) error {
	for _, it := range o {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

const (
	flagLimitHasOffset uint8 = 1 << iota
	flagLimitOffsetVar
	flagLimitLimitVar
)

type LimitNode struct {
	flag   uint8
	offset int64
	limit  int64
}

func (ln *LimitNode) SetOffsetVar() {
	ln.flag |= flagLimitOffsetVar
}

func (ln *LimitNode) SetLimitVar() {
	ln.flag |= flagLimitLimitVar
}

func (ln *LimitNode) SetHasOffset() {
	ln.flag |= flagLimitHasOffset
}

func (ln *LimitNode) HasOffset() bool {
	return ln.flag&flagLimitHasOffset != 0
}

func (ln *LimitNode) IsOffsetVar() bool {
	return ln.flag&flagLimitOffsetVar != 0
}

func (ln *LimitNode) IsLimitVar() bool {
	return ln.flag&flagLimitLimitVar != 0
}

func (ln *LimitNode) Offset() int64 {
	return ln.offset
}

func (ln *LimitNode) Limit() int64 {
	return ln.limit
}

func (ln *LimitNode) SetLimit(n int64) {
	ln.limit = n
}

func (ln *LimitNode) SetOffset(n int64) {
	ln.offset = n
}

type GroupByNode struct {
	RollUp bool
	Items  []*GroupByItem
}

func (g *GroupByNode) InTables(tables map[string]struct{}) error {
	for _, it := range g.Items {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

var (
	_ Statement = (*SelectStatement)(nil)
	_ Statement = (*DeleteStatement)(nil)
	_ Statement = (*UpdateStatement)(nil)
)

const (
	Sdistinct SelectSpec = "DISTINCT"
	Sall      SelectSpec = "ALL"
)

type SelectSpec string

type SelectStatement struct {
	Select      []SelectElement
	SelectSpecs []SelectSpec
	From        []*TableSourceNode
	Where       ExpressionNode
	GroupBy     *GroupByNode
	Having      ExpressionNode
	OrderBy     OrderByNode
	Limit       *LimitNode
}

func (ss *SelectStatement) HasSubQuery() bool {
	for _, it := range ss.From {
		if it.SubQuery() != nil {
			return true
		}
	}
	return false
}

func (ss *SelectStatement) Validate() error {
	tables := make(map[string]struct{})
	for _, it := range ss.From {
		alias := it.Alias()

		if len(alias) > 0 {
			tables[alias] = struct{}{}
		} else if tn := it.TableName(); tn != nil {
			tables[tn.Suffix()] = struct{}{}
			continue
		}

		if sq := it.SubQuery(); sq != nil {
			switch val := sq.(type) {
			case *SelectStatement:
				if err := val.Validate(); err != nil {
					return err
				}
			case *UnionSelectStatement:
				if err := val.Validate(); err != nil {
					return err
				}
			}
		}
	}

	for _, sel := range ss.Select {
		if err := sel.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid SELECT clause")
		}
	}

	if ss.Where != nil {
		if err := ss.Where.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid WHERE clause")
		}
	}

	if ss.OrderBy != nil {
		if err := ss.OrderBy.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid ORDER BY clause")
		}
	}

	if ss.GroupBy != nil {
		if err := ss.OrderBy.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid GROUP BY clause")
		}
	}

	return nil
}

func (ss *SelectStatement) GetSQLType() SQLType {
	return Squery
}

const (
	_flagDeleteLowPriority uint8 = 1 << iota
	_flagDeleteQuick
	_flagDeleteIgnore
)

type DeleteStatement struct {
	flag    uint8
	From    []*TableSourceNode
	Where   ExpressionNode
	OrderBy OrderByNode
	Limit   *LimitNode
}

func (ds *DeleteStatement) Validate() error {
	return nil
}

func (ds *DeleteStatement) GetSQLType() SQLType {
	return Sdelete
}

func (ds *DeleteStatement) IsLowPriority() bool {
	return ds.flag&_flagDeleteLowPriority != 0
}

func (ds *DeleteStatement) IsQuick() bool {
	return ds.flag&_flagDeleteQuick != 0
}

func (ds *DeleteStatement) IsIgnore() bool {
	return ds.flag&_flagDeleteIgnore != 0
}

func (ds *DeleteStatement) enableLowPriority() {
	ds.flag |= _flagDeleteLowPriority
}

func (ds *DeleteStatement) enableQuick() {
	ds.flag |= _flagDeleteQuick
}

func (ds *DeleteStatement) enableIgnore() {
	ds.flag |= _flagDeleteIgnore
}

type UpdateElement struct {
	Column ColumnNameExpressionAtom
	Value  ExpressionNode
}

const (
	_flagUpdateLowPriority uint8 = 1 << iota
	_flagUpdateIgnore
)

type UpdateStatement struct {
	flag       uint8
	Table      TableName
	TableAlias string
	Updated    []*UpdateElement
	Where      ExpressionNode
	OrderBy    OrderByNode
	Limit      *LimitNode
}

func (u *UpdateStatement) Validate() error {
	return nil
}

func (u *UpdateStatement) IsEnableLowPriority() bool {
	return u.flag&_flagUpdateLowPriority != 0
}

func (u *UpdateStatement) IsIgnore() bool {
	return u.flag&_flagUpdateIgnore != 0
}

func (u *UpdateStatement) enableLowPriority() {
	u.flag |= _flagUpdateLowPriority
}

func (u *UpdateStatement) enableIgnore() {
	u.flag |= _flagUpdateIgnore
}

func (u *UpdateStatement) GetSQLType() SQLType {
	return Supdate
}

const (
	_flagInsertIgnore uint8 = 1 << iota
	_flagInsertLowPriority
	_flagInsertDelayed
	_flagInsertHighPriority
	_flagInsertSetSyntax
)

var (
	_ BaseInsertValuesStatement = (*InsertStatement)(nil)
	_ BaseInsertValuesStatement = (*ReplaceStatement)(nil)
	_ BaseInsertSelectStatement = (*InsertSelectStatement)(nil)
	_ BaseInsertSelectStatement = (*ReplaceSelectStatement)(nil)
)

type BaseInsertStatement interface {
	Statement
	IsSetSyntax() bool
	IsIgnore() bool
	Priority() (string, bool)
	Columns() []string
	Table() TableName
}

type BaseInsertValuesStatement interface {
	BaseInsertStatement
	Values() [][]ExpressionNode
}

type BaseInsertSelectStatement interface {
	BaseInsertStatement
	Select() *SelectStatement
}

type baseInsertStatement struct {
	flag    uint8
	table   TableName
	columns []string
}

func (b *baseInsertStatement) Table() TableName {
	return b.table
}

func (b *baseInsertStatement) Columns() []string {
	return b.columns
}

func (b *baseInsertStatement) IsSetSyntax() bool {
	return b.flag&_flagInsertSetSyntax != 0
}

func (b *baseInsertStatement) IsIgnore() bool {
	return b.flag&_flagInsertIgnore != 0
}

func (b *baseInsertStatement) Priority() (priority string, ok bool) {
	if b.flag&_flagInsertHighPriority != 0 {
		priority = "HIGH_PRIORITY"
		ok = true
	} else if b.flag&_flagInsertLowPriority != 0 {
		priority = "LOW_PRIORITY"
		ok = true
	} else if b.flag&_flagInsertDelayed != 0 {
		priority = "DELAYED"
		ok = true
	}
	return
}

func (b *baseInsertStatement) enableIgnore() {
	b.flag |= _flagInsertIgnore
}

func (b *baseInsertStatement) enableLowPriority() {
	b.flag |= _flagInsertLowPriority
}

func (b *baseInsertStatement) enableHighPriority() {
	b.flag |= _flagInsertHighPriority
}

func (b *baseInsertStatement) enableDelayedPriority() {
	b.flag |= _flagInsertDelayed
}

func (b *baseInsertStatement) enableSetSyntax() {
	b.flag |= _flagInsertSetSyntax
}

type ReplaceStatement struct {
	*baseInsertStatement
	values [][]ExpressionNode
}

func (r *ReplaceStatement) Validate() error {
	return nil
}

func (r *ReplaceStatement) Values() [][]ExpressionNode {
	return r.values
}

func (r *ReplaceStatement) GetSQLType() SQLType {
	return Sreplace
}

type InsertStatement struct {
	*baseInsertStatement
	duplicatedUpdates []*UpdateElement
	values            [][]ExpressionNode
}

func (is *InsertStatement) Validate() error {
	return nil
}

func (is *InsertStatement) DuplicatedUpdates() []*UpdateElement {
	return is.duplicatedUpdates
}

func (is *InsertStatement) Values() [][]ExpressionNode {
	return is.values
}

func (is *InsertStatement) GetSQLType() SQLType {
	return Sinsert
}

type ReplaceSelectStatement struct {
	*baseInsertStatement
	sel *SelectStatement
}

func (r *ReplaceSelectStatement) Validate() error {
	return nil
}

func (r *ReplaceSelectStatement) Select() *SelectStatement {
	return r.sel
}

func (r *ReplaceSelectStatement) GetSQLType() SQLType {
	return Sreplace
}

type InsertSelectStatement struct {
	*baseInsertStatement
	sel *SelectStatement
}

func (is *InsertSelectStatement) Validate() error {
	return nil
}

func (is *InsertSelectStatement) Select() *SelectStatement {
	return is.sel
}

func (is *InsertSelectStatement) GetSQLType() SQLType {
	return Sinsert
}
