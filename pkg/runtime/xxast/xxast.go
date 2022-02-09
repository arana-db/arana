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
	"log"
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
			nSelect = cc.convFieldList(stmt.Fields)
			nFrom   = convFrom(stmt.From)
			nWhere  = toExpressionNode(cc.convExpr(stmt.Where))
			nLimit  = cc.convLimit(stmt.Limit)
		)

		return &SelectStatement{
			Select:      nSelect,
			SelectSpecs: nil,
			From:        nFrom,
			Where:       nWhere,
			Limit:       nLimit,
		}, nil
	case *ast.DeleteStmt:
		var (
			//TODO Now only support single table delete clause, need to fill flag OrderBy field
			nTable = convFrom(stmt.TableRefs)[0].source.(TableName)[0]
			nWhere = toExpressionNode(cc.convExpr(stmt.Where))
			nLimit = cc.convLimit(stmt.Limit)
		)
		return &DeleteStatement{
			Table: TableName{nTable},
			Where: nWhere,
			Limit: nLimit,
		}, nil
	case *ast.InsertStmt:
		var (
			nTable   = convFrom(stmt.Table)[0].source.(TableName)[0]
			nColumns = convInsertColumns(stmt.Columns)
		)

		return &InsertStatement{
			baseInsertStatement: &baseInsertStatement{
				table:   TableName{nTable},
				columns: nColumns,
			},
		}, nil
	}
	return nil, nil
}

func convInsertColumns(columnNames []*ast.ColumnName) []string {
	var result = make([]string, len(columnNames))
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
		tn := &TableSourceNode{
			source: TableName([]string{val.Name.String()}),
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
	var vis tblRefsVisitor
	from.Accept(&vis)
	return vis.v
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
			}
		}
	}
	return ret
}

func (cc *convCtx) convLimit(li *ast.Limit) *LimitNode {
	if li == nil {
		return nil
	}
	n := &LimitNode{
		offset: int64(li.Offset.(ast.ValueExpr).GetValue().(uint64)),
		limit:  int64(li.Count.(ast.ValueExpr).GetValue().(uint64)),
	}
	return n
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
	default:
		panic(fmt.Sprintf("todo: support %T!", node))
	}
}

func (cc *convCtx) convFuncCallExpr(expr *ast.FuncCallExpr) PredicateNode {
	var (
		fnName = expr.FnName.String()
		args   []*FunctionArg
	)

	toArg := func(arg ast.ExprNode) *FunctionArg {
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
			}
		case *BinaryComparisonPredicateNode:
			return &FunctionArg{
				typ:   FunctionArgExpression,
				value: &PredicateExpressionNode{P: next},
			}
		default:
			log.Printf("next: %T\n", next)
		}
		panic("unreachable")
	}

	for _, it := range expr.Args {
		args = append(args, toArg(it))
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
	node := cc.convExpr(expr.Expr)
	atom := &NestedExpressionAtom{
		First: &PredicateExpressionNode{
			P: node.(PredicateNode),
		},
	}
	return &AtomPredicateNode{
		A: atom,
	}
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

func (cc *convCtx) convValueExpr(expr ast.ValueExpr) PredicateNode {
	var atom ExpressionAtom
	switch t := expr.(type) {
	case *test_driver.ParamMarkerExpr:
		atom = VariableExpressionAtom(cc.getParamIndex())
	default:
		val := t.GetValue()
		atom = &ConstantExpressionAtom{Inner: val}
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
