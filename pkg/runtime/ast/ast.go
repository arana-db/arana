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
	"fmt"
	"strconv"
	"strings"
)

import (
	"github.com/arana-db/parser"
	"github.com/arana-db/parser/ast"
	"github.com/arana-db/parser/mysql"
	"github.com/arana-db/parser/opcode"
	"github.com/arana-db/parser/test_driver"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/runtime/logical"
)

var _opcode2comparison = map[opcode.Op]cmp.Comparison{
	opcode.EQ: cmp.Ceq,
	opcode.NE: cmp.Cne,
	opcode.LT: cmp.Clt,
	opcode.GT: cmp.Cgt,
	opcode.LE: cmp.Clte,
	opcode.GE: cmp.Cgte,
}

type (
	parseOption struct {
		charset   string
		collation string
	}

	ParseOption func(*parseOption)
)

// WithCharset sets the charset.
func WithCharset(charset string) ParseOption {
	return func(option *parseOption) {
		option.charset = charset
	}
}

// WithCollation sets the collation.
func WithCollation(collation string) ParseOption {
	return func(option *parseOption) {
		option.collation = collation
	}
}

// FromStmtNode converts raw ast node to Statement.
func FromStmtNode(node ast.StmtNode) (Statement, error) {
	var cc convCtx
	switch stmt := node.(type) {
	case *ast.SelectStmt:
		return cc.convSelectStmt(stmt), nil
	case *ast.SetOprStmt:
		return cc.convUnionStmt(stmt), nil
	case *ast.DeleteStmt:
		if stmt.IsMultiTable {
			return nil, errors.New("todo: DELETE with multiple tables")
		}
		return cc.convDeleteStmt(stmt), nil
	case *ast.InsertStmt:
		return cc.convInsertStmt(stmt), nil
	case *ast.UpdateStmt:
		return cc.convUpdateStmt(stmt), nil
	case *ast.ShowStmt:
		return cc.convShowStmt(stmt), nil
	case *ast.ExplainStmt:
		result, err := FromStmtNode(stmt.Stmt)
		if err != nil {
			return nil, err
		}
		switch tgt := result.(type) {
		case *ShowColumns:
			return &DescribeStatement{Table: tgt.TableName, Column: tgt.Column}, nil
		default:
			return &ExplainStatement{Target: tgt}, nil
		}
	case *ast.TruncateTableStmt:
		return cc.convTruncateTableStmt(stmt), nil
	case *ast.DropTableStmt:
		return cc.convDropTableStmt(stmt), nil
	case *ast.AlterTableStmt:
		return cc.convAlterTableStmt(stmt), nil
	case *ast.DropIndexStmt:
		return cc.convDropIndexStmt(stmt), nil
	case *ast.DropTriggerStmt:
		return cc.convDropTrigger(stmt), nil
	case *ast.CreateIndexStmt:
		return cc.convCreateIndexStmt(stmt), nil
	default:
		return nil, errors.Errorf("unimplement: stmt type %T!", stmt)
	}
}

func (cc *convCtx) convDropIndexStmt(stmt *ast.DropIndexStmt) *DropIndexStatement {
	var tableName TableName
	if db := stmt.Table.Schema.O; len(db) > 0 {
		tableName = append(tableName, db)
	}
	tableName = append(tableName, stmt.Table.Name.O)
	return &DropIndexStatement{
		IfExists:  stmt.IfExists,
		IndexName: stmt.IndexName,
		Table:     tableName,
	}
}

func (cc *convCtx) convCreateIndexStmt(stmt *ast.CreateIndexStmt) *CreateIndexStatement {
	var tableName TableName
	if db := stmt.Table.Schema.O; len(db) > 0 {
		tableName = append(tableName, db)
	}
	tableName = append(tableName, stmt.Table.Name.O)

	keys := make([]*IndexPartSpec, len(stmt.IndexPartSpecifications))
	for i, k := range stmt.IndexPartSpecifications {
		keys[i] = &IndexPartSpec{
			Column: cc.convColumn(k.Column),
			Expr:   toExpressionNode(cc.convExpr(k.Expr)),
		}
	}

	return &CreateIndexStatement{
		Table:     tableName,
		IndexName: stmt.IndexName,
		Keys:      keys,
	}
}

func (cc *convCtx) convAlterTableStmt(stmt *ast.AlterTableStmt) *AlterTableStatement {
	var tableName TableName
	if db := stmt.Table.Schema.O; len(db) > 0 {
		tableName = append(tableName, db)
	}
	tableName = append(tableName, stmt.Table.Name.O)

	var specs []*AlterTableSpecStatement
	for _, spec := range stmt.Specs {
		switch spec.Tp {
		case ast.AlterTableDropColumn:
			specs = append(specs, &AlterTableSpecStatement{
				Tp:            AlterTableDropColumn,
				OldColumnName: cc.convColumn(spec.OldColumnName),
			})
		case ast.AlterTableAddColumns:
			specs = append(specs, &AlterTableSpecStatement{
				Tp:         AlterTableAddColumns,
				NewColumns: cc.convColumnDef(spec.NewColumns),
				Position:   cc.convColumnPostition(spec.Position),
			})
		case ast.AlterTableAddConstraint:
			specs = append(specs, &AlterTableSpecStatement{
				Tp:         AlterTableAddConstraint,
				Constraint: cc.convConstraint(spec.Constraint),
			})
		case ast.AlterTableChangeColumn:
			specs = append(specs, &AlterTableSpecStatement{
				Tp:            AlterTableChangeColumn,
				OldColumnName: cc.convColumn(spec.OldColumnName),
				NewColumns:    cc.convColumnDef(spec.NewColumns),
				Position:      cc.convColumnPostition(spec.Position),
			})
		case ast.AlterTableModifyColumn:
			specs = append(specs, &AlterTableSpecStatement{
				Tp:         AlterTableModifyColumn,
				NewColumns: cc.convColumnDef(spec.NewColumns),
				Position:   cc.convColumnPostition(spec.Position),
			})
		case ast.AlterTableRenameTable:
			var newTable TableName
			if db := spec.NewTable.Schema.O; len(db) > 0 {
				newTable = append(newTable, db)
			}
			newTable = append(newTable, spec.NewTable.Name.O)
			specs = append(specs, &AlterTableSpecStatement{
				Tp:       AlterTableRenameTable,
				NewTable: newTable,
			})
		case ast.AlterTableRenameColumn:
			specs = append(specs, &AlterTableSpecStatement{
				Tp:            AlterTableRenameColumn,
				OldColumnName: cc.convColumn(spec.OldColumnName),
				NewColumnName: cc.convColumn(spec.NewColumnName),
			})
		}
	}
	return &AlterTableStatement{
		Table: tableName,
		Specs: specs,
	}
}

func (cc *convCtx) convColumnDef(cds []*ast.ColumnDef) []*ColumnDefine {
	var cols []*ColumnDefine
	for _, col := range cds {
		var opts []*ColumnOption
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionPrimaryKey:
				opts = append(opts, &ColumnOption{Tp: ColumnOptionPrimaryKey})
			case ast.ColumnOptionNotNull:
				opts = append(opts, &ColumnOption{Tp: ColumnOptionNotNull})
			case ast.ColumnOptionAutoIncrement:
				opts = append(opts, &ColumnOption{Tp: ColumnOptionAutoIncrement})
			case ast.ColumnOptionDefaultValue:
				opts = append(opts, &ColumnOption{
					Tp:   ColumnOptionDefaultValue,
					Expr: toExpressionNode(cc.convExpr(opt.Expr)),
				})
			case ast.ColumnOptionUniqKey:
				opts = append(opts, &ColumnOption{Tp: ColumnOptionUniqKey})
			case ast.ColumnOptionNull:
				opts = append(opts, &ColumnOption{Tp: ColumnOptionNull})
			case ast.ColumnOptionComment:
				opts = append(opts, &ColumnOption{
					Tp:   ColumnOptionComment,
					Expr: toExpressionNode(cc.convExpr(opt.Expr)),
				})
			case ast.ColumnOptionCollate:
				opts = append(opts, &ColumnOption{
					Tp:     ColumnOptionComment,
					StrVal: opt.StrValue,
				})
			case ast.ColumnOptionColumnFormat:
				opts = append(opts, &ColumnOption{
					Tp:     ColumnOptionColumnFormat,
					StrVal: opt.StrValue,
				})
			case ast.ColumnOptionStorage:
				opts = append(opts, &ColumnOption{
					Tp:     ColumnOptionStorage,
					StrVal: opt.StrValue,
				})
			}
		}
		cols = append(cols, &ColumnDefine{
			Column:  cc.convColumn(col.Name),
			Tp:      strings.ToUpper(col.Tp.String()),
			Options: opts,
		})
	}
	return cols
}

func (cc *convCtx) convColumnPostition(p *ast.ColumnPosition) *ColumnPosition {
	var pos *ColumnPosition
	if p != nil {
		switch p.Tp {
		case ast.ColumnPositionFirst:
			pos = &ColumnPosition{
				Tp: ColumnPositionFirst,
			}
		case ast.ColumnPositionAfter:
			pos = &ColumnPosition{
				Tp:     ColumnPositionAfter,
				Column: cc.convColumn(p.RelativeColumn),
			}
		}
	}
	return pos
}

func (cc *convCtx) convConstraint(c *ast.Constraint) *Constraint {
	if c == nil {
		return nil
	}
	keys := make([]*IndexPartSpec, len(c.Keys))
	for i, k := range c.Keys {
		keys[i] = &IndexPartSpec{
			Column: cc.convColumn(k.Column),
			Expr:   toExpressionNode(cc.convExpr(k.Expr)),
		}
	}
	tp := ConstraintNoConstraint
	switch c.Tp {
	case ast.ConstraintPrimaryKey:
		tp = ConstraintPrimaryKey
	case ast.ConstraintKey:
		tp = ConstraintKey
	case ast.ConstraintIndex:
		tp = ConstraintIndex
	case ast.ConstraintUniq:
		tp = ConstraintUniq
	case ast.ConstraintUniqKey:
		tp = ConstraintUniqKey
	case ast.ConstraintUniqIndex:
		tp = ConstraintUniqIndex
	case ast.ConstraintFulltext:
		tp = ConstraintFulltext
	}
	return &Constraint{
		Tp:   tp,
		Name: c.Name,
		Keys: keys,
	}
}

func (cc *convCtx) convDropTableStmt(stmt *ast.DropTableStmt) *DropTableStatement {
	tables := make([]*TableName, len(stmt.Tables))
	for i, table := range stmt.Tables {
		tables[i] = &TableName{
			table.Name.String(),
		}
	}
	return &DropTableStatement{
		Tables: tables,
	}
}

func (cc *convCtx) convUpdateStmt(stmt *ast.UpdateStmt) *UpdateStatement {
	var ret UpdateStatement
	switch stmt.Priority {
	case mysql.LowPriority:
		ret.enableLowPriority()
	}
	if stmt.IgnoreErr {
		ret.enableIgnore()
	}

	var tableName TableName
	switch left := stmt.TableRefs.TableRefs.Left.(type) {
	case *ast.TableSource:
		switch source := left.Source.(type) {
		case *ast.TableName:
			if db := source.Schema.O; len(db) > 0 {
				tableName = append(tableName, db)
			}
			tableName = append(tableName, source.Name.O)
		}
	}

	if len(tableName) < 1 {
		panic("no table name found")
	}
	ret.Table = tableName

	var updated []*UpdateElement
	for _, it := range stmt.List {
		var next UpdateElement
		next.Column = cc.convColumn(it.Column)
		next.Value = toExpressionNode(cc.convExpr(it.Expr))
		updated = append(updated, &next)
	}

	ret.Updated = updated

	if stmt.Where != nil {
		ret.Where = toExpressionNode(cc.convExpr(stmt.Where))
	}

	if stmt.Order != nil {
		ret.OrderBy = cc.convOrderBy(stmt.Order)
	}

	if stmt.Limit != nil {
		ret.Limit = cc.convLimit(stmt.Limit)
	}

	return &ret
}

func (cc *convCtx) convColumn(col *ast.ColumnName) ColumnNameExpressionAtom {
	var ret []string
	if schema := col.Schema.O; len(schema) > 0 {
		ret = append(ret, schema)
	}
	if table := col.Table.O; len(table) > 0 {
		ret = append(ret, table)
	}
	ret = append(ret, col.Name.O)
	return ret
}

func (cc *convCtx) convUnionStmt(stmt *ast.SetOprStmt) *UnionSelectStatement {
	var ret UnionSelectStatement

	ret.First = cc.convSelectStmt(stmt.SelectList.Selects[0].(*ast.SelectStmt))
	for i := 1; i < len(stmt.SelectList.Selects); i++ {
		var (
			next = stmt.SelectList.Selects[i].(*ast.SelectStmt)
			item UnionStatementItem
		)
		item.Stmt = cc.convSelectStmt(next)

		switch *next.AfterSetOperator {
		case ast.UnionAll:
			item.Type = UnionTypeAll
		case ast.Union:
			item.Type = UnionTypeDistinct
		}
		ret.UnionStatementItems = append(ret.UnionStatementItems, &item)
	}

	return &ret
}

func (cc *convCtx) convSelectStmt(stmt *ast.SelectStmt) *SelectStatement {
	var ret SelectStatement

	if stmt.Distinct {
		ret.enableDistinct()
	}

	ret.Select = cc.convFieldList(stmt.Fields)
	ret.From = cc.convFrom(stmt.From)
	if stmt.Where != nil {
		ret.Where = toExpressionNode(cc.convExpr(stmt.Where))
	}
	ret.GroupBy = cc.convGroupBy(stmt.GroupBy)
	ret.Having = cc.convHaving(stmt.Having)
	ret.OrderBy = cc.convOrderBy(stmt.OrderBy)
	ret.Limit = cc.convLimit(stmt.Limit)

	if stmt.LockInfo != nil {
		switch stmt.LockInfo.LockType {
		case ast.SelectLockForUpdate:
			ret.enableForUpdate()
		case ast.SelectLockForShare:
			ret.enableLockInShareMode()
		}
	}

	return &ret
}

func (cc *convCtx) convDeleteStmt(stmt *ast.DeleteStmt) Statement {
	var ret DeleteStatement

	if stmt.IgnoreErr {
		ret.enableIgnore()
	}

	if stmt.Quick {
		ret.enableQuick()
	}

	switch stmt.Priority {
	case mysql.LowPriority:
		ret.enableLowPriority()
	}

	// TODO: Now only support single table delete clause, need to fill flag OrderBy field
	ret.Table = cc.convFrom(stmt.TableRefs)[0].TableName()

	if stmt.Where != nil {
		ret.Where = toExpressionNode(cc.convExpr(stmt.Where))
	}

	if stmt.Order != nil {
		ret.OrderBy = cc.convOrderBy(stmt.Order)
	}

	if stmt.Limit != nil {
		ret.Limit = cc.convLimit(stmt.Limit)
	}

	return &ret
}

func (cc *convCtx) convInsertStmt(stmt *ast.InsertStmt) Statement {
	var (
		bi     baseInsertStatement
		values [][]ExpressionNode
	)

	// extract table
	bi.Table = cc.convFrom(stmt.Table)[0].TableName()

	if stmt.IgnoreErr {
		bi.enableIgnore()
	}

	switch stmt.Priority {
	case mysql.LowPriority:
		bi.enableLowPriority()
	case mysql.HighPriority:
		bi.enableHighPriority()
	case mysql.DelayedPriority:
		bi.enableDelayedPriority()
	}

	if stmt.Setlist == nil { // INSERT INTO xxx(...) VALUES (...)
		bi.Columns = convInsertColumns(stmt.Columns)
		values = make([][]ExpressionNode, 0, len(stmt.Lists))
		for _, row := range stmt.Lists {
			next := make([]ExpressionNode, 0, len(row))
			for _, col := range row {
				next = append(next, toExpressionNode(cc.convExpr(col)))
			}
			values = append(values, next)
		}
	} else { // INSERT INTO xxx SET xx=xx,...
		bi.enableSetSyntax() // mark as SET mode

		bi.Columns = make([]string, 0, len(stmt.Setlist))
		next := make([]ExpressionNode, 0, len(stmt.Setlist))
		for _, set := range stmt.Setlist {
			bi.Columns = append(bi.Columns, set.Column.Name.O)
			next = append(next, toExpressionNode(cc.convExpr(set.Expr)))
		}

		values = [][]ExpressionNode{next}
	}

	if stmt.IsReplace {
		return &ReplaceStatement{
			baseInsertStatement: &bi,
			Values:              values,
		}
	}

	var updates []*UpdateElement
	if stmt.OnDuplicate != nil {
		updates = make([]*UpdateElement, 0, len(stmt.OnDuplicate))
		for _, it := range stmt.OnDuplicate {
			updates = append(updates, &UpdateElement{
				Column: []string{it.Column.Name.O},
				Value:  toExpressionNode(cc.convExpr(it.Expr)),
			})
		}
	}

	if stmt.Select != nil {
		switch v := stmt.Select.(type) {
		case *ast.SelectStmt:
			return &InsertSelectStatement{
				baseInsertStatement: &bi,
				sel:                 cc.convSelectStmt(v),
				duplicatedUpdates:   updates,
			}
		case *ast.SetOprStmt:
			return &InsertSelectStatement{
				baseInsertStatement: &bi,
				unionSel:            cc.convUnionStmt(v),
				duplicatedUpdates:   updates,
			}
		}
	}

	return &InsertStatement{
		baseInsertStatement: &bi,
		Values:              values,
		DuplicatedUpdates:   updates,
	}
}

func (cc *convCtx) convTruncateTableStmt(node *ast.TruncateTableStmt) Statement {
	return &TruncateStatement{
		Table: []string{node.Table.Name.O},
	}
}

func (cc *convCtx) convShowStmt(node *ast.ShowStmt) Statement {
	toIn := func(node *ast.ShowStmt) (string, bool) {
		if node.DBName == "" {
			return "", false
		}
		return node.DBName, true
	}
	toFrom := func(node *ast.ShowStmt) (FromTable, bool) {
		if node.Table == nil {
			return "", false
		}
		return FromTable(node.Table.Name.String()), true
	}
	toWhere := func(node *ast.ShowStmt) (ExpressionNode, bool) {
		if node.Where == nil {
			return nil, false
		}
		return toExpressionNode(cc.convExpr(node.Where)), true
	}
	toShowLike := func(node *ast.ShowStmt) (PredicateNode, bool) {
		if node.Pattern == nil {
			return nil, false
		}
		return cc.convPatternLikeExpr(node.Pattern), true
	}
	toLike := func(node *ast.ShowStmt) (string, bool) {
		if node.Pattern == nil {
			return "", false
		}
		return node.Pattern.Pattern.(ast.ValueExpr).GetValue().(string), true
	}

	toBaseShow := func() *baseShow {
		var bs baseShow
		if like, ok := toShowLike(node); ok {
			bs.filter = like
		} else if where, ok := toWhere(node); ok {
			bs.filter = where
		} else if in, ok := toIn(node); ok {
			bs.filter = in
		} else if from, ok := toFrom(node); ok {
			bs.filter = from
		}
		return &bs
	}

	switch node.Tp {
	case ast.ShowTopology:
		return &ShowTopology{baseShow: toBaseShow()}
	case ast.ShowOpenTables:
		return &ShowOpenTables{baseShow: toBaseShow()}
	case ast.ShowTables:
		return &ShowTables{baseShow: toBaseShow()}
	case ast.ShowDatabases:
		return &ShowDatabases{baseShow: toBaseShow()}
	case ast.ShowCollation:
		return &ShowCollation{baseShow: toBaseShow()}
	case ast.ShowCreateTable:
		return &ShowCreate{
			typ: ShowCreateTypeTable,
			tgt: node.Table.Name.O,
		}
	case ast.ShowIndex:
		ret := &ShowIndex{
			TableName: []string{node.Table.Name.O},
		}
		if where, ok := toWhere(node); ok {
			ret.where = where
		}
		return ret
	case ast.ShowColumns:
		ret := &ShowColumns{
			TableName: []string{node.Table.Name.O},
		}
		if node.Column != nil {
			ret.Column = node.Column.Name.O
		}
		if node.Extended {
			ret.flag |= scFlagExtended
		}
		if node.Full {
			ret.flag |= scFlagFull
		}
		if like, ok := toLike(node); ok {
			ret.like.Valid, ret.like.String = true, like
		}
		return ret
	case ast.ShowVariables:
		ret := &ShowVariables{}
		if node.Full {
			ret.flag |= scFlagFull
		}
		if like, ok := toLike(node); ok {
			ret.like.Valid = true
			ret.like.String = like
		}
		return ret
	case ast.ShowStatus:
		ret := &ShowStatus{
			baseShow: toBaseShow(),
			global:   node.GlobalScope,
		}
		return ret
	default:
		panic(fmt.Sprintf("unimplement: show type %v!", node.Tp))
	}
}

func convInsertColumns(columnNames []*ast.ColumnName) []string {
	results := make([]string, 0, len(columnNames))
	for _, cn := range columnNames {
		results = append(results, cn.Name.O)
	}
	return results
}

// ParseSelect parses the SQL string to SelectStatement.
func ParseSelect(sql string, options ...ParseOption) ([]*hint.Hint, *SelectStatement, error) {
	h, s, err := Parse(sql, options...)
	if err != nil {
		return nil, nil, err
	}

	stmt, ok := s.(*SelectStatement)
	if !ok {
		return nil, nil, errors.Errorf("incorrect statement type: expect=%T, actual=%T", (*SelectStatement)(nil), s)
	}

	return h, stmt, nil
}

// Parse parses the SQL string to Statement.
func Parse(sql string, options ...ParseOption) ([]*hint.Hint, Statement, error) {
	var o parseOption
	for _, it := range options {
		it(&o)
	}

	p := parser.New()
	s, hintStrs, err := p.ParseOneStmtHints(sql, o.charset, o.collation)
	if err != nil {
		return nil, nil, err
	}

	stmt, err := FromStmtNode(s)
	if err != nil {
		return nil, nil, err
	}

	if len(hintStrs) < 1 {
		return nil, stmt, nil
	}

	hints := make([]*hint.Hint, 0, len(hintStrs))
	for _, it := range hintStrs {
		var h *hint.Hint
		if h, err = hint.Parse(it); err != nil {
			return nil, nil, errors.WithStack(err)
		}
		hints = append(hints, h)
	}

	return hints, stmt, nil
}

// MustParse parses the SQL string to Statement, panic if failed.
func MustParse(sql string) ([]*hint.Hint, Statement) {
	hints, stmt, err := Parse(sql)
	if err != nil {
		panic(err.Error())
	}
	return hints, stmt
}

type convCtx struct {
	paramsCnt int32
}

func (cc *convCtx) getParamIndex() int32 {
	cur := cc.paramsCnt
	cc.paramsCnt++
	return cur
}

func (cc *convCtx) convFrom(from *ast.TableRefsClause) (ret []*TableSourceNode) {
	if from == nil {
		return
	}

	transform := func(input ast.ResultSetNode) *TableSourceNode {
		if input == nil {
			return nil
		}
		switch val := input.(type) {
		case *ast.TableSource:
			var target TableSourceNode
			target.Alias = val.AsName.O
			switch source := val.Source.(type) {
			case *ast.TableName:
				cc.convTableName(source, &target)
			case *ast.SelectStmt:
				target.source = cc.convSelectStmt(source)
			case *ast.SetOprStmt:
				target.source = cc.convUnionStmt(source)
			default:
				panic(fmt.Sprintf("unimplement: table source %T!", source))
			}
			return &target
		default:
			panic(fmt.Sprintf("unimplement: table refs %T!", val))
		}
	}

	var (
		left  = transform(from.TableRefs.Left)
		right = transform(from.TableRefs.Right)
	)

	var on ExpressionNode
	if from.TableRefs.On != nil {
		on = toExpressionNode(cc.convExpr(from.TableRefs.On.Expr))
	}

	if on == nil {
		ret = append(ret, left)
		return
	}

	var jn JoinNode

	jn.Left = left
	jn.Right = right
	jn.On = on

	switch from.TableRefs.Tp {
	case ast.LeftJoin:
		jn.Typ = LeftJoin
	case ast.RightJoin:
		jn.Typ = RightJoin
	case ast.CrossJoin:
		jn.Typ = InnerJoin
	}

	if from.TableRefs.NaturalJoin {
		jn.natural = true
	}

	ret = append(ret, &TableSourceNode{source: &jn})

	return
}

func (cc *convCtx) convGroupBy(by *ast.GroupByClause) *GroupByNode {
	if by == nil || len(by.Items) < 1 {
		return nil
	}

	ret := &GroupByNode{
		Items: make([]*GroupByItem, 0, len(by.Items)),
	}

	for _, it := range by.Items {
		var next GroupByItem
		if it.Desc {
			next.flag = flagGroupByOrderDesc | flagGroupByHasOrder
		}
		next.expr = toExpressionNode(cc.convExpr(it.Expr))
		ret.Items = append(ret.Items, &next)
	}

	return ret
}

func (cc *convCtx) convOrderBy(orderBy *ast.OrderByClause) (ret OrderByNode) {
	if orderBy == nil || len(orderBy.Items) < 1 {
		return nil
	}

	for _, it := range orderBy.Items {
		var next OrderByItem
		next.Desc = it.Desc
		switch val := cc.convExpr(it.Expr).(type) {
		case ExpressionAtom:
			next.Expr = val
		case *AtomPredicateNode:
			next.Expr = val.A
		default:
			panic(fmt.Sprintf("unimplement: ORDER_BY_ITEM type %T!", val))
		}
		ret = append(ret, &next)
	}

	return
}

func (cc *convCtx) convFieldList(node *ast.FieldList) []SelectElement {
	ret := make([]SelectElement, 0, len(node.Fields))
	for _, field := range node.Fields {
		if field.WildCard != nil {
			ret = append(ret, &SelectElementAll{})
			continue
		}

		alias := field.AsName.String()
		switch t := cc.convExpr(field.Expr).(type) {
		case *AtomPredicateNode:
			switch a := t.A.(type) {
			case ColumnNameExpressionAtom:
				ret = append(ret, &SelectElementColumn{
					Name:  a,
					alias: alias,
				})
			case *FunctionCallExpressionAtom:
				ret = append(ret, &SelectElementFunction{
					inner: a.F,
					alias: alias,
				})
			case *ConstantExpressionAtom:
				ret = append(ret, &SelectElementExpr{
					inner: exprAtomToNode(a),
					alias: alias,
				})
			case *MathExpressionAtom:
				ret = append(ret, &SelectElementExpr{
					inner: exprAtomToNode(a),
					alias: alias,
				})
			case *UnaryExpressionAtom:
				ret = append(ret, &SelectElementExpr{
					inner: exprAtomToNode(a),
					alias: alias,
				})
			case *NestedExpressionAtom:
				ret = append(ret, &SelectElementExpr{
					inner: exprAtomToNode(a),
					alias: alias,
				})
			case *SystemVariableExpressionAtom:
				ret = append(ret, &SelectElementExpr{
					inner: exprAtomToNode(a),
					alias: alias,
				})
			default:
				panic(fmt.Sprintf("todo: unsupported select element type %T!", a))
			}
		}
	}
	return ret
}

func (cc *convCtx) convLimit(li *ast.Limit) *LimitNode {
	if li == nil {
		return nil
	}

	var n LimitNode
	if offset := li.Offset; offset != nil {
		n.SetHasOffset()
		switch t := offset.(type) {
		case *test_driver.ParamMarkerExpr:
			n.SetOffsetVar()
			n.SetOffset(int64(cc.getParamIndex()))
		case ast.ValueExpr:
			n.SetOffset(int64(t.GetValue().(uint64)))
		default:
			panic(fmt.Sprintf("todo: unsupported limit offset type %T!", t))
		}
	}

	switch t := li.Count.(type) {
	case *test_driver.ParamMarkerExpr:
		n.SetLimitVar()
		n.SetLimit(int64(cc.getParamIndex()))
	case ast.ValueExpr:
		n.SetLimit(int64(t.GetValue().(uint64)))
	default:
		panic(fmt.Sprintf("todo: unsupported limit offset type %T!", t))
	}

	return &n
}

func (cc *convCtx) convExpr(expr ast.ExprNode) interface{} {
	if expr == nil {
		return nil
	}
	switch node := expr.(type) {
	case *ast.BinaryOperationExpr:
		return cc.convBinaryOperationExpr(node)
	case *ast.ColumnNameExpr:
		return convColumnNameExpr(node)
	case *ast.PatternInExpr:
		return cc.convPatternInExpr(node)
	case *ast.BetweenExpr:
		return cc.convBetweenExpr(node)
	case *ast.ParenthesesExpr:
		return cc.convParenthesesExpr(node)
	case *ast.PatternLikeExpr:
		return cc.convPatternLikeExpr(node)
	case ast.ValueExpr:
		return cc.convValueExpr(node)
	case *ast.UnaryOperationExpr:
		return cc.convUnaryExpr(node)
	case *ast.AggregateFuncExpr:
		return cc.convAggregateFuncExpr(node)
	case *ast.CaseExpr:
		return cc.convCaseExpr(node)
	case *ast.FuncCallExpr:
		return cc.convFuncCallExpr(node)
	case *ast.FuncCastExpr:
		return cc.convCastExpr(node)
	case *ast.IsNullExpr:
		return cc.convIsNullExpr(node)
	case *ast.VariableExpr:
		return cc.convVariableExpr(node)
	case *ast.PatternRegexpExpr:
		return cc.convRegexpExpr(node)
	case *ast.TimeUnitExpr:
		return cc.convTimeUnitExpr(node)
	default:
		panic(fmt.Sprintf("unimplement: expr node type %T!", node))
	}
}

func (cc *convCtx) convVariableExpr(node *ast.VariableExpr) PredicateNode {
	return &AtomPredicateNode{
		A: &SystemVariableExpressionAtom{
			Name: node.Name,
		},
	}
}

func (cc *convCtx) convCastExpr(node *ast.FuncCastExpr) PredicateNode {
	var (
		f    CastFunction
		left = cc.convExpr(node.Expr)
	)

	switch node.FunctionType {
	case ast.CastFunction:
		f.isCast = true
	case ast.CastConvertFunction:
	case ast.CastBinaryOperator:
		panic("unknown cast binary operator!")
	}

	var cast strings.Builder
	node.Tp.FormatAsCastType(&cast, true)

	// WORKAROUND: fix original cast string
	if strings.EqualFold("binary binary", cast.String()) {
		cast.Reset()
		cast.WriteString("BINARY")
	}

	var typ ConvertDataType
	if err := typ.Parse(cast.String()); err != nil {
		panic(err.Error())
	}

	f.src = toExpressionNode(left)
	f.cast = &typ

	return &AtomPredicateNode{
		A: &FunctionCallExpressionAtom{
			F: &f,
		},
	}
}

func (cc *convCtx) convCaseExpr(node *ast.CaseExpr) PredicateNode {
	caseBlock := cc.convExpr(node.Value)

	branches := make([][2]*FunctionArg, 0, len(node.WhenClauses))
	for _, it := range node.WhenClauses {
		var branch [2]*FunctionArg
		branch[0] = cc.toArg(it.Expr)
		branch[1] = cc.toArg(it.Result)
		branches = append(branches, branch)
	}

	elseBlock := cc.toArg(node.ElseClause)

	f := &CaseWhenElseFunction{
		branches:  branches,
		elseBlock: elseBlock,
	}

	if caseBlock != nil {
		switch it := caseBlock.(type) {
		case PredicateNode:
			f.caseBlock = &PredicateExpressionNode{P: it}
		default:
			panic(fmt.Sprintf("unimplement: case when block type %T!", it))
		}
	}

	return &AtomPredicateNode{
		A: &FunctionCallExpressionAtom{
			F: f,
		},
	}
}

func (cc *convCtx) convAggregateFuncExpr(node *ast.AggregateFuncExpr) PredicateNode {
	var f AggrFunction

	f.name = strings.ToUpper(node.F)
	if node.Distinct {
		f.aggregator = Distinct
	}

	switch f.name {
	case "COUNT":
		if len(node.Args) < 1 {
			f.EnableCountStar()
		}
		fallthrough
	default:
		for _, it := range node.Args {
			f.args = append(f.args, cc.toArg(it))
		}
	}

	return &AtomPredicateNode{
		A: &FunctionCallExpressionAtom{
			F: &f,
		},
	}
}

func (cc *convCtx) convFuncCallExpr(expr *ast.FuncCallExpr) PredicateNode {
	fnName := strings.ToUpper(expr.FnName.O)

	// NOTICE: tidb-parser cannot process CONVERT('foobar' USING utf8).
	// It should be a CastFunc, but now will be parsed as a FuncCall.
	// We should do some transform work.
	var inner interface{}
	switch fnName {
	case "CONVERT":
		_ = expr.Args[1]
		var (
			first  = toExpressionNode(cc.convExpr(expr.Args[0]))
			second = cc.convExpr(expr.Args[1])
		)
		inner = &CastFunction{
			src:  first,
			cast: second.(*AtomPredicateNode).A.(*ConstantExpressionAtom).Value().(string),
		}
	default:
		var (
			isTimeUnit bool
			args       = make([]*FunctionArg, 0, len(expr.Args))
		)
		for _, it := range expr.Args {
			next := cc.toArg(it)
			args = append(args, next)

			isTimeUnit = false
			if next.Type == FunctionArgConstant {
				_, isTimeUnit = next.Value.(ast.TimeUnitType)
			}
		}

		if isTimeUnit {
			args[len(args)-2] = &FunctionArg{
				Type: FunctionArgExpression,
				Value: &PredicateExpressionNode{
					P: &AtomPredicateNode{
						A: &IntervalExpressionAtom{
							Unit:  args[len(args)-1].Value.(ast.TimeUnitType),
							Value: cc.convExpr(expr.Args[len(args)-2]).(PredicateNode),
						},
					},
				},
			}
			args = args[:len(args)-1]
		}

		inner = &Function{
			typ:  Fspec,
			name: fnName,
			args: args,
		}
	}

	return &AtomPredicateNode{
		A: &FunctionCallExpressionAtom{
			F: inner,
		},
	}
}

func (cc *convCtx) toArg(arg ast.ExprNode) *FunctionArg {
	if arg == nil {
		return nil
	}
	switch next := cc.convExpr(arg).(type) {
	case *AtomPredicateNode:
		switch atom := next.A.(type) {
		case ColumnNameExpressionAtom:
			return &FunctionArg{
				Type:  FunctionArgColumn,
				Value: atom,
			}
		case *ConstantExpressionAtom:
			return &FunctionArg{
				Type:  FunctionArgConstant,
				Value: atom.Value(),
			}
		case VariableExpressionAtom:
			return &FunctionArg{
				Type:  FunctionArgExpression,
				Value: &PredicateExpressionNode{P: next},
			}
		case *UnaryExpressionAtom:
			return &FunctionArg{
				Type:  FunctionArgExpression,
				Value: &PredicateExpressionNode{P: next},
			}
		case *MathExpressionAtom:
			return &FunctionArg{
				Type:  FunctionArgExpression,
				Value: &PredicateExpressionNode{P: next},
			}
		case *FunctionCallExpressionAtom:
			return &FunctionArg{
				Type:  FunctionArgExpression,
				Value: &PredicateExpressionNode{P: next},
			}
		default:
			panic(fmt.Sprintf("unimplement: function arg atom type %T!", atom))
		}
	case *BinaryComparisonPredicateNode:
		return &FunctionArg{
			Type:  FunctionArgExpression,
			Value: &PredicateExpressionNode{P: next},
		}
	case *RegexpPredicationNode:
		return &FunctionArg{
			Type:  FunctionArgExpression,
			Value: &PredicateExpressionNode{P: next},
		}
	default:
		panic(fmt.Sprintf("unimplement: function arg type %T!", next))
	}
}

func (cc *convCtx) convPatternLikeExpr(expr *ast.PatternLikeExpr) PredicateNode {
	var (
		left, _  = cc.convExpr(expr.Expr).(PredicateNode)
		right, _ = cc.convExpr(expr.Pattern).(PredicateNode)
	)
	return &LikePredicateNode{
		Not:   expr.Not,
		Left:  left,
		Right: right,
	}
}

func (cc *convCtx) convParenthesesExpr(expr *ast.ParenthesesExpr) PredicateNode {
	var atom ExpressionAtom
	switch node := cc.convExpr(expr.Expr).(type) {
	case ExpressionNode:
		atom = &NestedExpressionAtom{
			First: node,
		}
	case PredicateNode:
		atom = &NestedExpressionAtom{
			First: &PredicateExpressionNode{
				P: node,
			},
		}
	default:
		panic(fmt.Sprintf("unimplement: nested type %T!", node))
	}

	return &AtomPredicateNode{A: atom}
}

func (cc *convCtx) convBetweenExpr(expr *ast.BetweenExpr) PredicateNode {
	var (
		key   = cc.convExpr(expr.Expr)
		left  = cc.convExpr(expr.Left)
		right = cc.convExpr(expr.Right)
	)
	return &BetweenPredicateNode{
		Not:   expr.Not,
		Key:   key.(PredicateNode),
		Left:  left.(PredicateNode),
		Right: right.(PredicateNode),
	}
}

func (cc *convCtx) convPatternInExpr(expr *ast.PatternInExpr) PredicateNode {
	key := cc.convExpr(expr.Expr)
	list := make([]ExpressionNode, 0, len(expr.List))
	for _, it := range expr.List {
		pn := cc.convExpr(it).(PredicateNode)
		list = append(list, &PredicateExpressionNode{P: pn})
	}

	return &InPredicateNode{
		Not: expr.Not,
		P:   key.(PredicateNode),
		E:   list,
	}
}

func (cc *convCtx) convUnaryExpr(expr *ast.UnaryOperationExpr) PredicateNode {
	var atom interface{}

	switch t := cc.convExpr(expr.V).(type) {
	case ExpressionAtom:
		atom = t
	case *AtomPredicateNode:
		atom = t.A
	case *BinaryComparisonPredicateNode:
		atom = t
	default:
		panic(fmt.Sprintf("unsupport unary inner expr type %T!", t))
	}

	var sb strings.Builder
	expr.Op.Format(&sb)

	return &AtomPredicateNode{
		A: &UnaryExpressionAtom{
			Operator: sb.String(),
			Inner:    atom,
		},
	}
}

func (cc *convCtx) convValueExpr(expr ast.ValueExpr) PredicateNode {
	var atom ExpressionAtom
	switch t := expr.(type) {
	case *test_driver.ParamMarkerExpr:
		atom = VariableExpressionAtom(cc.getParamIndex())
	default:
		switch val := t.GetValue().(type) {
		case *test_driver.MyDecimal:
			// TODO: decimal or float?
			f, _ := strconv.ParseFloat(val.String(), 64)
			atom = &ConstantExpressionAtom{Inner: f}
		default:
			if val == nil {
				atom = &ConstantExpressionAtom{Inner: Null{}}
			} else {
				atom = &ConstantExpressionAtom{Inner: val}
			}
		}
	}
	return &AtomPredicateNode{A: atom}
}

func (cc *convCtx) convTimeUnitExpr(node *ast.TimeUnitExpr) PredicateNode {
	return &AtomPredicateNode{
		A: &ConstantExpressionAtom{
			Inner: node.Unit,
		},
	}
}

func convColumnNameExpr(expr *ast.ColumnNameExpr) PredicateNode {
	var (
		table  = expr.Name.Table.O
		column = expr.Name.Name.O
	)

	if len(table) < 1 {
		return &AtomPredicateNode{
			A: ColumnNameExpressionAtom([]string{column}),
		}
	}

	return &AtomPredicateNode{
		A: ColumnNameExpressionAtom([]string{table, column}),
	}
}

func (cc *convCtx) convIsNullExpr(node *ast.IsNullExpr) PredicateNode {
	var (
		left  = cc.convExpr(node.Expr)
		right = &ConstantExpressionAtom{
			Inner: Null{},
		}
	)

	ret := &BinaryComparisonPredicateNode{
		Left: left.(PredicateNode),
		Right: &AtomPredicateNode{
			A: right,
		},
	}

	if node.Not {
		ret.Op = _opcode2comparison[opcode.NE]
	} else {
		ret.Op = _opcode2comparison[opcode.EQ]
	}

	return ret
}

func (cc *convCtx) convRegexpExpr(node *ast.PatternRegexpExpr) PredicateNode {
	var (
		left  = cc.convExpr(node.Expr).(PredicateNode)
		right = cc.convExpr(node.Pattern).(PredicateNode)
	)
	return &RegexpPredicationNode{
		Left:  left,
		Right: right,
		Not:   node.Not,
	}
}

func (cc *convCtx) convBinaryOperationExpr(expr *ast.BinaryOperationExpr) interface{} {
	var (
		left  = cc.convExpr(expr.L)
		right = cc.convExpr(expr.R)
	)

	switch expr.Op {
	case opcode.Plus, opcode.Minus, opcode.Div, opcode.Mul, opcode.Mod:
		return &AtomPredicateNode{A: &MathExpressionAtom{
			Left:     left.(*AtomPredicateNode).A,
			Operator: expr.Op.Literal(),
			Right:    right.(*AtomPredicateNode).A,
		}}
	case opcode.EQ, opcode.NE, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		op := _opcode2comparison[expr.Op]

		if !isColumnAtom(left.(PredicateNode)) && isColumnAtom(right.(PredicateNode)) {
			// do reverse:
			// 1 = uid === uid = 1
			// 1 <> uid === uid <> 1
			// 1 < uid === uid > 1
			// 1 > uid === uid < 1
			// 1 <= uid === uid >= 1
			// 1 >= uid === uid <= 1
			left, right = right, left
			switch expr.Op {
			case opcode.GT:
				op = _opcode2comparison[opcode.LT]
			case opcode.GE:
				op = _opcode2comparison[opcode.LE]
			case opcode.LT:
				op = _opcode2comparison[opcode.GT]
			case opcode.LE:
				op = _opcode2comparison[opcode.GE]
			}
		}
		return &BinaryComparisonPredicateNode{
			Left:  left.(PredicateNode),
			Right: right.(PredicateNode),
			Op:    op,
		}
	case opcode.LogicAnd:
		return &LogicalExpressionNode{
			Op:    logical.Land,
			Left:  toExpressionNode(left),
			Right: toExpressionNode(right),
		}
	case opcode.LogicOr:
		return &LogicalExpressionNode{
			Op:    logical.Lor,
			Left:  toExpressionNode(left),
			Right: toExpressionNode(right),
		}
	default:
		panic(fmt.Sprintf("todo: support opcode %s!", expr.Op.String()))
	}
}

func (cc *convCtx) convHaving(having *ast.HavingClause) ExpressionNode {
	if having == nil {
		return nil
	}
	return toExpressionNode(cc.convExpr(having.Expr))
}

func (cc *convCtx) convTableName(val *ast.TableName, tgt *TableSourceNode) {
	var (
		schema     = val.Schema.String()
		name       = val.Name.String()
		partitions []string
		indexHints []*IndexHint
	)

	var tableName TableName

	if len(schema) < 1 {
		tableName = []string{name}
	} else {
		tableName = []string{schema, name}
	}

	// parse partitions
	for _, it := range val.PartitionNames {
		partitions = append(partitions, it.O)
	}

	// parse index
	for _, it := range val.IndexHints {
		var next IndexHint
		switch it.HintType {
		case ast.HintUse:
			next.action = indexHintActionUse
		case ast.HintIgnore:
			next.action = indexHintActionIgnore
		case ast.HintForce:
			next.action = indexHintActionForce
		}

		switch it.HintScope {
		case ast.HintForGroupBy:
			next.indexHintType = indexHintTypeGroupBy
		case ast.HintForJoin:
			next.indexHintType = indexHintTypeJoin
		case ast.HintForOrderBy:
			next.indexHintType = indexHintTypeOrderBy
		}
		next.indexes = make([]string, 0, len(it.IndexNames))
		for _, indexName := range it.IndexNames {
			next.indexes = append(next.indexes, indexName.O)
		}
		indexHints = append(indexHints, &next)
	}

	tgt.source = tableName
	tgt.IndexHints = indexHints
	tgt.Partitions = partitions
}

func (cc *convCtx) convDropTrigger(stmt *ast.DropTriggerStmt) *DropTriggerStatement {
	var tableName TableName
	if db := stmt.Trigger.Schema.O; len(db) > 0 {
		tableName = append(tableName, db)
	}
	tableName = append(tableName, stmt.Trigger.Name.O)
	return &DropTriggerStatement{Table: tableName, IfExists: stmt.IfExists}
}

func toExpressionNode(src interface{}) ExpressionNode {
	if src == nil {
		return nil
	}
	switch v := src.(type) {
	case PredicateNode:
		return &PredicateExpressionNode{
			P: v,
		}
	case ExpressionNode:
		return v
	default:
		panic(fmt.Sprintf("todo: convert to ExpressionNode: type=%T", src))
	}
}

func exprAtomToNode(atom ExpressionAtom) ExpressionNode {
	return &PredicateExpressionNode{
		P: &AtomPredicateNode{
			A: atom,
		},
	}
}

func isColumnAtom(expr PredicateNode) bool {
	switch t := expr.(type) {
	case *AtomPredicateNode:
		if _, ok := t.A.(ColumnNameExpressionAtom); ok {
			return true
		}
	}
	return false
}
