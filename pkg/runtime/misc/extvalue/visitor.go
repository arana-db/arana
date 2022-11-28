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

package extvalue

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

import (
	"github.com/arana-db/parser/opcode"

	gxbig "github.com/dubbogo/gost/math/big"

	perrors "github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/cmp"
)

var errNotValue = errors.New("cannot extract value from non-value object")

func IsErrNotSupportedValue(err error) bool {
	return perrors.Is(err, errNotValue)
}

type valueVisitor struct {
	ast.BaseVisitor
	args []interface{}
}

func (vv *valueVisitor) VisitPredicateExpression(node *ast.PredicateExpressionNode) (interface{}, error) {
	return node.P.Accept(vv)
}

func (vv *valueVisitor) VisitPredicateBinaryComparison(node *ast.BinaryComparisonPredicateNode) (interface{}, error) {
	l, err := node.Left.Accept(vv)
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	r, err := node.Right.Accept(vv)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	// TODO: should check number
	c := strings.Compare(fmt.Sprint(l), fmt.Sprint(r))

	switch node.Op {
	case cmp.Ceq:
		return c == 0, nil
	case cmp.Clt:
		return c < 0, nil
	case cmp.Clte:
		return c <= 0, nil
	case cmp.Cgt:
		return c > 0, nil
	case cmp.Cgte:
		return c >= 0, nil
	case cmp.Cne:
		return c != 0, nil
	default:
		panic("unreachable")
	}
}

func (vv *valueVisitor) VisitPredicateAtom(node *ast.AtomPredicateNode) (interface{}, error) {
	return node.A.Accept(vv)
}

func (vv *valueVisitor) VisitAtomColumn(node ast.ColumnNameExpressionAtom) (interface{}, error) {
	return nil, nil
}

func (vv *valueVisitor) VisitAtomConstant(node *ast.ConstantExpressionAtom) (interface{}, error) {
	return node.Value(), nil
}

func (vv *valueVisitor) VisitAtomFunction(node *ast.FunctionCallExpressionAtom) (interface{}, error) {
	return node.F.Accept(vv)
}

func (vv *valueVisitor) VisitAtomNested(node *ast.NestedExpressionAtom) (interface{}, error) {
	return node.First.Accept(vv)
}

func (vv *valueVisitor) VisitAtomUnary(node *ast.UnaryExpressionAtom) (interface{}, error) {
	v, err := node.Inner.Accept(vv)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	switch node.Operator {
	case "-":
		switch it := v.(type) {
		case int8:
			return -1 * it, nil
		case int16:
			return -1 * it, nil
		case int32:
			return -1 * it, nil
		case int64:
			return -1 * it, nil
		case float32:
			return -1 * it, nil
		case float64:
			return -1 * it, nil
		case decimal.Decimal:
			return it.Mul(decimal.NewFromInt(int64(-1))), nil
		}
	}

	// TODO: support all unary operators
	return nil, perrors.Errorf("unsupported unary '%s' for value %T(%v)", node.Operator, v, v)
}

func (vv *valueVisitor) VisitAtomMath(node *ast.MathExpressionAtom) (interface{}, error) {
	l, err := node.Left.Accept(vv)
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	r, err := node.Right.Accept(vv)
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	switch node.Operator {
	case opcode.Plus.Literal():
		return computeMath(l, r, func(x, y decimal.Decimal) (decimal.Decimal, error) {
			return x.Add(y), nil
		})
	case opcode.Minus.Literal():
		return computeMath(l, r, func(x, y decimal.Decimal) (decimal.Decimal, error) {
			return x.Sub(y), nil
		})
	case opcode.Mul.Literal():
		return computeMath(l, r, func(x, y decimal.Decimal) (decimal.Decimal, error) {
			return x.Mul(y), nil
		})
	case opcode.Div.Literal():
		return computeMath(l, r, func(x, y decimal.Decimal) (decimal.Decimal, error) {
			return x.Div(y), nil
		})
	case opcode.IntDiv.Literal():
		ret, err := computeMath(l, r, func(x, y decimal.Decimal) (decimal.Decimal, error) {
			return x.Div(y), nil
		})
		if err != nil {
			return nil, err
		}
		return ret.IntPart(), nil
	default:
		// TODO: need implementation
		return nil, perrors.Errorf("unsupported math opcode '%s'", node.Operator)
	}
}

func (vv *valueVisitor) VisitAtomSystemVariable(node *ast.SystemVariableExpressionAtom) (interface{}, error) {
	return nil, errNotValue
}

func (vv *valueVisitor) VisitAtomVariable(node ast.VariableExpressionAtom) (interface{}, error) {
	return vv.tidyValue(vv.args[node.N()])
}

func (vv *valueVisitor) VisitAtomInterval(node *ast.IntervalExpressionAtom) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (vv *valueVisitor) tidyValue(in interface{}) (interface{}, error) {
	switch val := in.(type) {
	case *gxbig.Decimal:
		d, _ := decimal.NewFromString(val.String())
		return d, nil
	default:
		return val, nil
	}
}

func (vv *valueVisitor) VisitFunction(node *ast.Function) (interface{}, error) {
	fn, ok := proto.GetFunc(node.Name())
	if !ok {
		return nil, perrors.Errorf("no such function '%s'", node.Name())
	}

	if len(node.Args()) < fn.NumInput() {
		return nil, perrors.Errorf("incorrect parameter count in the call to native function '%s'", node.Name())
	}

	var args []proto.Valuer
	for i := range node.Args() {
		next := node.Args()[i]
		args = append(args, proto.FuncValuer(func(_ context.Context) (proto.Value, error) {
			return next.Accept(vv)
		}))
	}

	res, err := fn.Apply(context.Background(), args...)
	if err != nil {
		return nil, perrors.Wrapf(err, "failed to call function '%s'", node.Name())
	}
	return vv.tidyValue(res)
}

func (vv *valueVisitor) VisitFunctionCast(node *ast.CastFunction) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (vv *valueVisitor) VisitFunctionCaseWhenElse(node *ast.CaseWhenElseFunction) (interface{}, error) {
	var (
		caseValue interface{}
		whenValue interface{}
		err       error
	)
	if c := node.CaseBlock; c != nil {
		if caseValue, err = c.Accept(vv); err != nil {
			return nil, perrors.Wrap(err, "cannot eval value of CASE")
		}
	}

	for _, b := range node.BranchBlocks {
		if whenValue, err = b.When.Accept(vv); err != nil {
			return nil, perrors.WithStack(err)
		}

		// 1. CASE WHEN x>=60 then 'PASS' ...
		// 2. CASE x WHEN 2 then 'OK' ...
		if (caseValue == nil && whenValue != nil && reflect.ValueOf(whenValue).IsValid()) ||
			(caseValue != nil && whenValue != nil && proto.PrintValue(caseValue) == proto.PrintValue(whenValue)) {
			return b.Then.Accept(vv)
		}
	}

	// eval ELSE node
	if node.ElseBlock != nil {
		return node.ElseBlock.Accept(vv)
	}

	// return NULL
	return nil, nil
}

func (vv *valueVisitor) VisitFunctionArg(node *ast.FunctionArg) (interface{}, error) {
	switch node.Type {
	case ast.FunctionArgColumn:
		return nil, errNotValue
	case ast.FunctionArgExpression:
		return node.Value.(ast.ExpressionNode).Accept(vv)
	case ast.FunctionArgConstant:
		return node.Value, nil
	case ast.FunctionArgFunction:
		return node.Value.(*ast.Function).Accept(vv)
	case ast.FunctionArgAggrFunction:
		return nil, errNotValue
	case ast.FunctionArgCaseWhenElseFunction:
		return node.Value.(*ast.CaseWhenElseFunction).Accept(vv)
	case ast.FunctionArgCastFunction:
		return node.Value.(*ast.CastFunction).Accept(vv)
	default:
		panic("unreachable")
	}
}

func computeMath(a, b interface{}, c func(x, y decimal.Decimal) (decimal.Decimal, error)) (ret decimal.Decimal, err error) {
	var x, y decimal.Decimal
	if x, err = decimal.NewFromString(fmt.Sprint(a)); err != nil {
		return
	}
	if y, err = decimal.NewFromString(fmt.Sprint(b)); err != nil {
		return
	}
	ret, err = c(x, y)
	return
}
