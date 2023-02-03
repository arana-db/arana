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

package optimize

import (
	"strings"
)

import (
	"github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/runtime/logical"
	"github.com/arana-db/arana/pkg/runtime/misc/extvalue"
	rrule "github.com/arana-db/arana/pkg/runtime/rule"
)

var _ ast.Visitor = (*CalculateVisitor)(nil)

type CalculateVisitor struct {
	ast.BaseVisitor
	args []proto.Value
}

func NewXCalcualtor(args []proto.Value) *CalculateVisitor {
	return &CalculateVisitor{
		args: args,
	}
}

func (sd *CalculateVisitor) VisitLogicalExpression(node *ast.LogicalExpressionNode) (interface{}, error) {
	left, err := node.Left.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	right, err := node.Right.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ll, lr := left.(logical.Logical), right.(logical.Logical)
	switch node.Op {
	case logical.Lor:
		return ll.Or(lr), nil
	case logical.Land:
		return ll.And(lr), nil
	default:
		panic("unreachable")
	}
}

func (sd *CalculateVisitor) VisitNotExpression(node *ast.NotExpressionNode) (interface{}, error) {
	ret, err := node.E.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret.(logical.Logical).Not(), nil
}

func (sd *CalculateVisitor) VisitPredicateExpression(node *ast.PredicateExpressionNode) (interface{}, error) {
	return node.P.Accept(sd)
}
func (sd *CalculateVisitor) VisitSelectElementExpr(node *ast.SelectElementExpr) (interface{}, error) {
	switch node.Expression().(type) {
	case *ast.PredicateExpressionNode:
		return sd.VisitPredicateExpression(node.Expression().(*ast.PredicateExpressionNode))
	case *ast.LogicalExpressionNode:
		return sd.VisitLogicalExpression(node.Expression().(*ast.LogicalExpressionNode))
	}
	return node.Expression().Accept(sd)
}

func (sd *CalculateVisitor) VisitSelectElementFunction(node *ast.SelectElementFunction) (interface{}, error) {
	nodeF := node.Function().(*ast.Function)
	val, err := extvalue.Compute(nodeF, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return rrule.AlwaysTrueLogical, nil
		}
		return nil, errors.WithStack(err)
	}
	return sd.fromConstant(val)
}

func (sd *CalculateVisitor) VisitPredicateAtom(node *ast.AtomPredicateNode) (interface{}, error) {
	return node.A.Accept(sd)
}

func (sd *CalculateVisitor) VisitPredicateBetween(node *ast.BetweenPredicateNode) (interface{}, error) {
	key := node.Key.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	l, err := extvalue.Compute(node.Left, sd.args...)
	if err != nil {
		return nil, err
	}

	r, err := extvalue.Compute(node.Right, sd.args...)
	if err != nil {
		return nil, err
	}

	if node.Not {
		// convert: f NOT BETWEEN a AND b -> f < a OR f > b
		k1 := rrule.NewKeyed(key.Suffix(), cmp.Clt, l)
		k2 := rrule.NewKeyed(key.Suffix(), cmp.Cgt, r)
		return k1.ToLogical().Or(k2.ToLogical()), nil
	}

	// convert: f BETWEEN a AND b -> f >= a AND f <= b
	k1 := rrule.NewKeyed(key.Suffix(), cmp.Cgte, l)
	k2 := rrule.NewKeyed(key.Suffix(), cmp.Clte, r)
	return k1.ToLogical().And(k2.ToLogical()), nil
}

func (sd *CalculateVisitor) VisitPredicateBinaryComparison(node *ast.BinaryComparisonPredicateNode) (interface{}, error) {
	switch k := node.Left.(*ast.AtomPredicateNode).A.(type) {
	case ast.ColumnNameExpressionAtom:
		v, err := extvalue.Compute(node.Right, sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return rrule.AlwaysTrueLogical, nil
			}
			return nil, errors.WithStack(err)
		}
		return rrule.NewKeyed(k.Suffix(), node.Op, v).ToLogical(), nil
	}

	switch k := node.Right.(*ast.AtomPredicateNode).A.(type) {
	case ast.ColumnNameExpressionAtom:
		v, err := extvalue.Compute(node.Left, sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return rrule.AlwaysTrueLogical, nil
			}
			return nil, errors.WithStack(err)
		}
		return rrule.NewKeyed(k.Suffix(), node.Op, v).ToLogical(), nil
	}

	l, _ := extvalue.Compute(node.Left, sd.args...)
	r, _ := extvalue.Compute(node.Right, sd.args...)
	if l == nil || r == nil {
		return rrule.AlwaysTrueLogical, nil
	}

	var (
		isStr bool
		c     int
	)

	switch l.Family() {
	case proto.ValueFamilyString:
		switch r.Family() {
		case proto.ValueFamilyString:
			isStr = true
		}
	}

	if isStr {
		c = strings.Compare(l.String(), r.String())
	} else {
		x, err := l.Decimal()
		if err == nil {
			x = decimal.Zero
		}
		y, err := r.Decimal()
		if err == nil {
			y = decimal.Zero
		}
		switch {
		case x.GreaterThan(y):
			c = 1
		case x.LessThan(y):
			c = -1
		}
	}
	var bingo bool
	switch node.Op {
	case cmp.Ceq:
		bingo = c == 0
	case cmp.Cne:
		bingo = c != 0
	case cmp.Cgt:
		bingo = c > 0
	case cmp.Cgte:
		bingo = c >= 0
	case cmp.Clt:
		bingo = c < 0
	case cmp.Clte:
		bingo = c <= 0
	}

	if bingo {
		return rrule.AlwaysTrueLogical, nil
	}
	return rrule.AlwaysFalseLogical, nil
}

func (sd *CalculateVisitor) VisitPredicateIn(node *ast.InPredicateNode) (interface{}, error) {
	key := node.P.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	var ret logical.Logical
	for i := range node.E {
		actualValue, err := extvalue.Compute(node.E[i], sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return rrule.AlwaysTrueLogical, nil
			}
			return nil, errors.WithStack(err)
		}
		if node.Not {
			// convert: f NOT IN (a,b,c) -> f <> a AND f <> b AND f <> c
			ke := rrule.NewKeyed(key.Suffix(), cmp.Cne, actualValue)
			if ret == nil {
				ret = ke.ToLogical()
			} else {
				ret = ret.And(ke.ToLogical())
			}
		} else {
			// convert: f IN (a,b,c) -> f = a OR f = b OR f = c
			ke := rrule.NewKeyed(key.Suffix(), cmp.Ceq, actualValue)
			if ret == nil {
				ret = ke.ToLogical()
			} else {
				ret = ret.Or(ke.ToLogical())
			}
		}
	}

	return ret, nil
}

func (sd *CalculateVisitor) VisitAtomColumn(node ast.ColumnNameExpressionAtom) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *CalculateVisitor) VisitAtomConstant(node *ast.ConstantExpressionAtom) (interface{}, error) {
	v, err := proto.NewValue(node.Value())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	l, err := sd.fromConstant(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return l, nil
}

func (sd *CalculateVisitor) VisitAtomFunction(node *ast.FunctionCallExpressionAtom) (interface{}, error) {
	val, err := extvalue.Compute(node, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return rrule.AlwaysTrueLogical, nil
		}
		return nil, errors.WithStack(err)
	}
	return sd.fromConstant(val)
}

func (sd *CalculateVisitor) VisitAtomNested(node *ast.NestedExpressionAtom) (interface{}, error) {
	return node.First.Accept(sd)
}

func (sd *CalculateVisitor) VisitAtomUnary(node *ast.UnaryExpressionAtom) (interface{}, error) {
	return sd.fromValueNode(node)
}

func (sd *CalculateVisitor) VisitAtomMath(node *ast.MathExpressionAtom) (interface{}, error) {
	return sd.fromValueNode(node)
}

func (sd *CalculateVisitor) VisitAtomSystemVariable(node *ast.SystemVariableExpressionAtom) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *CalculateVisitor) VisitAtomVariable(node ast.VariableExpressionAtom) (interface{}, error) {
	return sd.fromConstant(sd.args[node.N()])
}

func (sd *CalculateVisitor) VisitAtomInterval(node *ast.IntervalExpressionAtom) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *CalculateVisitor) fromConstant(val proto.Value) (proto.Value, error) {
	if val.Family().IsNumberic() {
		return val, nil
	}
	return val, errors.New("Local Calculate error ! ")
}

func (sd *CalculateVisitor) fromValueNode(node ast.Node) (interface{}, error) {
	val, err := extvalue.Compute(node, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return rrule.AlwaysTrueLogical, nil
		}
		return nil, errors.WithStack(err)
	}
	return sd.fromConstant(val)
}
