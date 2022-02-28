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

package xxast

import (
	"fmt"
	"strconv"
	"strings"
)

import (
	"github.com/dubbogo/parser"
	"github.com/dubbogo/parser/ast"
	"github.com/dubbogo/parser/opcode"
	"github.com/dubbogo/parser/test_driver"

	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/cmp"
	"github.com/dubbogo/arana/pkg/runtime/logical"
)

var (
	_opcode2comparison = map[opcode.Op]cmp.Comparison{
		opcode.EQ: cmp.Ceq,
		opcode.NE: cmp.Cne,
		opcode.LT: cmp.Clt,
		opcode.GT: cmp.Cgt,
		opcode.LE: cmp.Clte,
		opcode.GE: cmp.Cgte,
	}

	_opcode2math = map[opcode.Op]string{
		opcode.Plus:  "+",
		opcode.Minus: "-",
		opcode.Div:   "/",
		opcode.Mul:   "*",
	}
)

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
		var (
			nSelect  = cc.convFieldList(stmt.Fields)
			nFrom    = convFrom(stmt.From)
			nWhere   = toExpressionNode(cc.convExpr(stmt.Where))
			nGroup   = cc.convGroupBy(stmt.GroupBy)
			nOrderBy = cc.convOrderBy(stmt.OrderBy)
			nLimit   = cc.convLimit(stmt.Limit)
		)

		return &SelectStatement{
			Select:      nSelect,
			SelectSpecs: nil,
			From:        nFrom,
			Where:       nWhere,
			OrderBy:     nOrderBy,
			GroupBy:     nGroup,
			Limit:       nLimit,
		}, nil
	case *ast.DeleteStmt:
		var (
			//TODO Now only support single table delete clause, need to fill flag OrderBy field
			nFrom  = convFrom(stmt.TableRefs)
			nWhere = toExpressionNode(cc.convExpr(stmt.Where))
			nLimit = cc.convLimit(stmt.Limit)
		)
		return &DeleteStatement{
			From:  nFrom,
			Where: nWhere,
			Limit: nLimit,
		}, nil
	case *ast.InsertStmt:
		var (
			nTable           = convFrom(stmt.Table)[0]
			nColumns         []string
			nValues          [][]ExpressionNode
			nUpdatedElements []*UpdateElement
		)
		if stmt.OnDuplicate != nil && len(stmt.OnDuplicate) != 0 {
			nUpdatedElements = cc.convAssignment(stmt.OnDuplicate)
		}
		if len(stmt.Setlist) != 0 {
			// insert into sink set a=b
			nValues = append(nValues, make([]ExpressionNode, 0, len(stmt.Setlist)))
			setList := stmt.Setlist
			for _, set := range setList {
				nColumns = append(nColumns, set.Column.Name.O)
				nValues[0] = append(nValues[0], &PredicateExpressionNode{
					cc.convExpr(set.Expr).(PredicateNode),
				})
			}
		} else {
			// insert into sink value(a, b)
			nColumns = convInsertColumns(stmt.Columns)
			for i, list := range stmt.Lists {
				nValues = append(nValues, make([]ExpressionNode, 0, len(list)))
				for _, elem := range list {
					nValues[i] = append(nValues[i], &PredicateExpressionNode{
						cc.convExpr(elem).(PredicateNode),
					})
				}
			}
		}

		return &InsertStatement{
			baseInsertStatement: &baseInsertStatement{
				table:   nTable.TableName(),
				columns: nColumns,
			},
			duplicatedUpdates: nUpdatedElements,
			values:            nValues,
		}, nil
	case *ast.UpdateStmt:
		var (
			nTable   = convFrom(stmt.TableRefs)[0]
			nExprs   = cc.convAssignment(stmt.List)
			nWhere   = toExpressionNode(cc.convExpr(stmt.Where))
			nLimit   = cc.convLimit(stmt.Limit)
			nOrderBy = cc.convOrderBy(stmt.Order)
		)
		return &UpdateStatement{
			flag:       0,
			Table:      nTable.TableName(),
			TableAlias: nTable.Alias(),
			Updated:    nExprs,
			Where:      nWhere,
			OrderBy:    nOrderBy,
			Limit:      nLimit,
		}, nil
	}
	return nil, nil
}

func convInsertColumns(columnNames []*ast.ColumnName) []string {
	var result = make([]string, 0, len(columnNames))
	for _, cn := range columnNames {
		result = append(result, cn.Name.O)
	}
	return result
}

// Parse parses the SQL string to Statement.
func Parse(sql string, options ...ParseOption) (Statement, error) {
	var o parseOption
	for _, it := range options {
		it(&o)
	}

	p := parser.New()
	s, err := p.ParseOneStmt(sql, o.charset, o.collation)
	if err != nil {
		return nil, errors.Wrap(err, "parse sql ast failed")
	}

	return FromStmtNode(s)
}

// MustParse parses the SQL string to Statement, panic if failed.
func MustParse(sql string) Statement {
	stmt, err := Parse(sql)
	if err != nil {
		panic(err.Error())
	}
	return stmt
}

type convCtx struct {
	paramsCnt int32
}

func (cc *convCtx) getParamIndex() int32 {
	cur := cc.paramsCnt
	cc.paramsCnt++
	return cur
}

type tblRefsVisitor struct {
	alias []string
	v     []*TableSourceNode
}

func (t *tblRefsVisitor) Enter(n ast.Node) (ast.Node, bool) {
	switch val := n.(type) {
	case *ast.TableSource:
		t.alias = append(t.alias, val.AsName.String())
	case *ast.TableName:
		var (
			schema = val.Schema.String()
			name   = val.Name.String()
		)

		var tableName TableName
		if len(schema) < 1 {
			tableName = []string{name}
		} else {
			tableName = []string{schema, name}
		}

		tn := &TableSourceNode{
			source: tableName,
			alias:  "",
		}
		if len(t.alias) > 0 {
			tn.alias = t.alias[len(t.alias)-1]
			t.alias = t.alias[:len(t.alias)-1]
		}
		t.v = append(t.v, tn)
	}
	return n, false
}

func (t tblRefsVisitor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func convFrom(from *ast.TableRefsClause) []*TableSourceNode {
	if from == nil {
		return nil
	}
	var vis tblRefsVisitor
	from.Accept(&vis)
	return vis.v
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
					name:  a,
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
			default:
				panic(fmt.Sprintf("todo: unsupported select element type %T!", a))
			}
		}
	}
	return ret
}

// convert assignment, like a = 1.
func (cc *convCtx) convAssignment(assignments []*ast.Assignment) []*UpdateElement {
	result := make([]*UpdateElement, 0, len(assignments))
	for _, assignment := range assignments {
		var nColumn ColumnNameExpressionAtom
		column := assignment.Column
		if column.Schema.O != "" {
			nColumn = append(nColumn, column.Schema.O)
		}
		if column.Table.O != "" {
			nColumn = append(nColumn, column.Table.O)
		}
		if column.Name.O != "" {
			nColumn = append(nColumn, column.Name.O)
		}
		nValue := cc.convExpr(assignment.Expr).(PredicateNode)
		result = append(result, &UpdateElement{
			Column: nColumn,
			Value:  &PredicateExpressionNode{nValue},
		})
	}
	return result
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
	case *ast.FuncCallExpr:
		return cc.convFuncCallExpr(node)
	case ast.ValueExpr:
		return cc.convValueExpr(node)
	case *ast.UnaryOperationExpr:
		return cc.convUnaryExpr(node)
	case *ast.AggregateFuncExpr:
		return cc.convAggregateFuncExpr(node)
	case *ast.CaseExpr:
		return cc.convCaseExpr(node)
	default:
		panic(fmt.Sprintf("unimplement: expr node type %T!", node))
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
	var (
		fnName = strings.ToUpper(node.F)
		args   = make([]*FunctionArg, 0, len(node.Args))
	)

	for _, it := range node.Args {
		args = append(args, cc.toArg(it))
	}

	f := &AggrFunction{
		name: fnName,
		args: args,
	}

	if node.Distinct {
		f.aggregator = AggregatorDistinct
	}
	// TODO: all?

	return &AtomPredicateNode{
		A: &FunctionCallExpressionAtom{
			F: f,
		},
	}
}

func (cc *convCtx) convFuncCallExpr(expr *ast.FuncCallExpr) PredicateNode {
	var (
		fnName = strings.ToUpper(expr.FnName.O)
		args   = make([]*FunctionArg, 0, len(expr.Args))
	)

	for _, it := range expr.Args {
		args = append(args, cc.toArg(it))
	}

	atom := &FunctionCallExpressionAtom{
		F: &Function{
			typ:  Fspec,
			name: fnName,
			args: args,
		},
	}

	return &AtomPredicateNode{A: atom}
}

func (cc *convCtx) toArg(arg ast.ExprNode) *FunctionArg {
	switch next := cc.convExpr(arg).(type) {
	case *AtomPredicateNode:
		switch atom := next.A.(type) {
		case ColumnNameExpressionAtom:
			return &FunctionArg{
				typ:   FunctionArgColumn,
				value: atom,
			}
		case *ConstantExpressionAtom:
			return &FunctionArg{
				typ:   FunctionArgConstant,
				value: atom.Value(),
			}
		case VariableExpressionAtom:
			return &FunctionArg{
				typ:   FunctionArgExpression,
				value: &PredicateExpressionNode{P: next},
			}
		case *UnaryExpressionAtom:
			return &FunctionArg{
				typ:   FunctionArgExpression,
				value: &PredicateExpressionNode{P: next},
			}
		case *MathExpressionAtom:
			return &FunctionArg{
				typ:   FunctionArgExpression,
				value: &PredicateExpressionNode{P: next},
			}
		default:
			panic(fmt.Sprintf("unimplement: function arg atom type %T!", atom))
		}
	case *BinaryComparisonPredicateNode:
		return &FunctionArg{
			typ:   FunctionArgExpression,
			value: &PredicateExpressionNode{P: next},
		}
	default:
		panic(fmt.Sprintf("unimplement: function arg type %T!", next))
	}
}

func (cc *convCtx) convPatternLikeExpr(expr *ast.PatternLikeExpr) PredicateNode {
	var (
		left  = cc.convExpr(expr.Expr)
		right = cc.convExpr(expr.Pattern)
	)
	return &LikePredicateNode{
		Not:   expr.Not,
		Left:  left.(PredicateNode),
		Right: right.(PredicateNode),
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
		not: expr.Not,
		P:   key.(PredicateNode),
		E:   list,
	}
}

func (cc *convCtx) convUnaryExpr(expr *ast.UnaryOperationExpr) PredicateNode {
	var atom ExpressionAtom

	switch t := cc.convExpr(expr.V).(type) {
	case ExpressionAtom:
		atom = t
	case *AtomPredicateNode:
		atom = t.A
	default:
		panic(fmt.Sprintf("unsupport unary inner expr type %T!", t))
	}

	var sb strings.Builder
	expr.Op.Format(&sb)

	return &AtomPredicateNode{A: &UnaryExpressionAtom{
		Operator: sb.String(),
		Inner:    atom,
	}}
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
			atom = &ConstantExpressionAtom{Inner: val}
		}

	}
	return &AtomPredicateNode{A: atom}
}

func convColumnNameExpr(expr *ast.ColumnNameExpr) PredicateNode {
	return &AtomPredicateNode{
		A: ColumnNameExpressionAtom([]string{expr.Name.String()}),
	}
}

func (cc *convCtx) convBinaryOperationExpr(expr *ast.BinaryOperationExpr) interface{} {
	var (
		left  = cc.convExpr(expr.L)
		right = cc.convExpr(expr.R)
	)

	switch expr.Op {
	case opcode.Plus, opcode.Minus, opcode.Div, opcode.Mul:
		return &AtomPredicateNode{A: &MathExpressionAtom{
			Left:     left.(*AtomPredicateNode).A,
			Operator: _opcode2math[expr.Op],
			Right:    right.(*AtomPredicateNode).A,
		}}
	case opcode.EQ, opcode.NE, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		return &BinaryComparisonPredicateNode{
			Left:  left.(PredicateNode),
			Right: right.(PredicateNode),
			Op:    _opcode2comparison[expr.Op],
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
