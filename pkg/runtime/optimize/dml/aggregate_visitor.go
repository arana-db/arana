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

package dml

import (
	"reflect"
	"strings"
)

import (
	"github.com/arana-db/parser/opcode"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
)

type aggregateVisitor struct {
	ast.AlwaysReturnSelfVisitor
	hasMapping   bool
	hasWeak      bool
	aggregations []*ast.SelectElementFunction
}

func (av *aggregateVisitor) VisitSelectStatement(node *ast.SelectStatement) (interface{}, error) {
	var rebuilds []ast.SelectElement
	for _, next := range node.Select {
		res, err := next.Accept(av)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		switch val := next.(type) {
		case *ext.WeakSelectElement:
			rebuilds = append(rebuilds, &ext.WeakSelectElement{
				SelectElement: res.(ast.SelectElement),
			})
			av.hasWeak = true
		case *ext.WeakAliasSelectElement:
			rebuilds = append(rebuilds, &ext.WeakAliasSelectElement{
				SelectElement: res.(ast.SelectElement),
				WeakAlias:     val.WeakAlias,
			})
		default:
			rebuilds = append(rebuilds, res.(ast.SelectElement))
		}
	}

	var (
		rf ast.RestoreFlag
		sb strings.Builder
	)
	for i := range av.aggregations {
		var (
			found bool
			cur   = av.aggregations[i].Function().(*ast.AggrFunction)
		)
	R:
		for _, rebuild := range rebuilds {
			var next *ast.AggrFunction

			switch t := rebuild.(type) {
			case *ast.SelectElementFunction:
				switch f := t.Function().(type) {
				case *ast.AggrFunction:
					next = f
				}
			case ext.SelectElementProvider:
				switch p := t.Prev().(type) {
				case *ast.SelectElementFunction:
					switch f := p.Function().(type) {
					case *ast.AggrFunction:
						next = f
					}
				}
			}

			if next == nil {
				continue
			}

			if cur.Name() != next.Name() {
				continue
			}
			_ = cur.Restore(rf, &sb, nil)
			curS := sb.String()
			sb.Reset()
			_ = next.Restore(rf, &sb, nil)
			nextS := sb.String()
			sb.Reset()
			if curS == nextS {
				found = true
				break R
			}
		}

		if found {
			continue
		}

		rebuilds = append(rebuilds, &ext.WeakSelectElement{
			SelectElement: av.aggregations[i],
		})
		av.hasWeak = true
	}

	node.Select = rebuilds

	return nil, nil
}

func (av *aggregateVisitor) VisitSelectElementFunction(node *ast.SelectElementFunction) (interface{}, error) {
	before := len(av.aggregations)
	v, err := node.Function().Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	switch t := v.(type) {
	case ast.ExpressionAtom:
		expr := &ast.PredicateExpressionNode{
			P: &ast.AtomPredicateNode{
				A: t,
			},
		}
		var vs ext.MappingSelectElement
		vs.SelectElement = node
		vs.Mapping = ast.NewSelectElementExpr(expr, "")
		av.hasMapping = true
		return &vs, nil
	default:
		switch f := node.Function().(type) {
		case *ast.Function:
			if after := len(av.aggregations); after > before {
				var vs ext.MappingSelectElement
				vs.SelectElement = node
				vs.Mapping = ast.NewSelectElementFunction(f, "")
				av.hasMapping = true
				return &vs, nil
			}
		case *ast.CaseWhenElseFunction:
			if after := len(av.aggregations); after > before {
				var vs ext.MappingSelectElement
				vs.SelectElement = node
				vs.Mapping = ast.NewSelectElementCaseWhenFunction(f, "")
				av.hasMapping = true
				return &vs, nil
			}
		}
		return node, nil
	}
}

func (av *aggregateVisitor) VisitSelectElementExpr(node *ast.SelectElementExpr) (interface{}, error) {
	before := len(av.aggregations)
	expr, err := node.Expression().Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// no aggregate function found, return directly
	if len(av.aggregations) == before {
		return node, nil
	}

	// build virtual field
	var vs ext.MappingSelectElement
	vs.SelectElement = node
	vs.Mapping = ast.NewSelectElementExpr(expr.(ast.ExpressionNode), "")

	av.hasMapping = true

	return &vs, nil
}

func (av *aggregateVisitor) VisitFunctionAggregate(node *ast.AggrFunction) (interface{}, error) {
	switch node.Name() {
	case ast.AggrAvg:
		var (
			sumFunc  = ast.NewAggrFunction(ast.AggrSum, "", node.Args())
			cntFunc  = ast.NewAggrFunction(ast.AggrCount, "", node.Args())
			sumField = ast.NewSelectElementAggrFunction(sumFunc, "")
			cntField = ast.NewSelectElementAggrFunction(cntFunc, "")
		)
		av.aggregations = append(av.aggregations, sumField, cntField)
		return &ast.MathExpressionAtom{
			Left:     &ast.FunctionCallExpressionAtom{F: sumFunc},
			Operator: opcode.Div.Literal(),
			Right:    &ast.FunctionCallExpressionAtom{F: cntFunc},
		}, nil
	case ast.AggrCount:
		if aggregator, ok := node.Aggregator(); ok {
			// TODO: count distinct
			return nil, errors.Errorf("todo: handle COUNT with aggregator '%s'", aggregator)
		}
		fallthrough
	default:
		newborn := ast.NewSelectElementAggrFunction(node, "")
		av.aggregations = append(av.aggregations, newborn)
		return node, nil
	}
}

func (av *aggregateVisitor) VisitPredicateExpression(node *ast.PredicateExpressionNode) (interface{}, error) {
	p, err := node.P.Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if reflect.DeepEqual(p, node.P) {
		return node, nil
	}

	newborn := *node
	newborn.P = p.(ast.PredicateNode)
	return &newborn, nil
}

func (av *aggregateVisitor) VisitAtomUnary(node *ast.UnaryExpressionAtom) (interface{}, error) {
	n, err := node.Inner.Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if reflect.DeepEqual(n, node.Inner) {
		return node, nil
	}

	newborn := *node
	newborn.Inner = n.(ast.Node)
	return &newborn, nil
}

func (av *aggregateVisitor) VisitPredicateAtom(node *ast.AtomPredicateNode) (interface{}, error) {
	a, err := node.A.Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if reflect.DeepEqual(a, node.A) {
		return node, nil
	}

	newborn := *node
	newborn.A = a.(ast.ExpressionAtom)

	return &newborn, nil
}

func (av *aggregateVisitor) VisitAtomMath(node *ast.MathExpressionAtom) (interface{}, error) {
	left, err := node.Left.Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	right, err := node.Right.Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if reflect.DeepEqual(left, node.Left) && reflect.DeepEqual(right, node.Right) {
		return node, nil
	}

	newborn := *node
	newborn.Left = left.(ast.ExpressionAtom)
	newborn.Right = right.(ast.ExpressionAtom)

	return &newborn, nil
}

func (av *aggregateVisitor) VisitAtomFunction(node *ast.FunctionCallExpressionAtom) (interface{}, error) {
	f, err := node.F.Accept(av)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if reflect.DeepEqual(f, node.F) {
		return node, nil
	}

	switch next := f.(type) {
	case *ast.Function, *ast.AggrFunction, *ast.CaseWhenElseFunction, *ast.CastFunction:
		newborn := *node
		newborn.F = f.(ast.Node)
		return &newborn, nil
	case ast.ExpressionAtom:
		return next, nil
	default:
		panic("unreachable")
	}
}

func (av *aggregateVisitor) VisitFunction(node *ast.Function) (interface{}, error) {
	args := node.Args()
	for i := range args {
		next, err := args[i].Accept(av)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if arg2 := next.(*ast.FunctionArg); arg2 != args[i] {
			args[i] = arg2
		}
	}
	return node, nil
}

func (av *aggregateVisitor) VisitFunctionArg(node *ast.FunctionArg) (interface{}, error) {
	var (
		next = node.Value
		err  error
	)
	switch node.Type {
	case ast.FunctionArgExpression:
		next, err = node.Value.(ast.ExpressionNode).Accept(av)
	case ast.FunctionArgFunction:
		next, err = node.Value.(*ast.Function).Accept(av)
	case ast.FunctionArgAggrFunction:
		next, err = node.Value.(*ast.AggrFunction).Accept(av)
	case ast.FunctionArgCaseWhenElseFunction:
		next, err = node.Value.(*ast.CaseWhenElseFunction).Accept(av)
	case ast.FunctionArgCastFunction:
		next, err = node.Value.(*ast.CastFunction).Accept(av)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	node.Value = next

	return node, nil
}

func (av *aggregateVisitor) VisitFunctionCaseWhenElse(node *ast.CaseWhenElseFunction) (interface{}, error) {
	var (
		next interface{}
		err  error
	)
	if c := node.CaseBlock; c != nil {
		if next, err = c.Accept(av); err != nil {
			return nil, errors.WithStack(err)
		}
		node.CaseBlock = next.(ast.ExpressionNode)
	}

	for _, b := range node.BranchBlocks {
		if next, err = b.When.Accept(av); err != nil {
			return nil, errors.WithStack(err)
		}
		b.When = next.(*ast.FunctionArg)

		if next, err = b.Then.Accept(av); err != nil {
			return nil, errors.WithStack(err)
		}
		b.Then = next.(*ast.FunctionArg)
	}

	if node.ElseBlock != nil {
		if next, err = node.ElseBlock.Accept(av); err != nil {
			return nil, errors.WithStack(err)
		}
		node.ElseBlock = next.(*ast.FunctionArg)
	}

	return node, nil
}
