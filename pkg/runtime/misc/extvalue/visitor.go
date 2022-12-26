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
	"strings"
)

import (
	"github.com/arana-db/parser/opcode"

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
	args []proto.Value
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

	var lv, rv proto.Value
	switch v := l.(type) {
	case proto.Value:
		lv = v
	}
	switch v := r.(type) {
	case proto.Value:
		rv = v
	}

	if lv == nil || rv == nil {
		return nil, nil
	}

	var (
		c     int
		isStr bool
	)
	switch lv.Family() {
	case proto.ValueFamilyString:
		switch rv.Family() {
		case proto.ValueFamilyString:
			isStr = true
		}
	}

	if isStr {
		c = strings.Compare(lv.String(), rv.String())
	} else {
		x, err := lv.Decimal()
		if err != nil {
			x = decimal.Zero
		}
		y, err := rv.Decimal()
		if err != nil {
			y = decimal.Zero
		}

		switch {
		case x.GreaterThan(y):
			c = 1
		case x.LessThan(y):
			c = -1
		}
	}

	var b bool
	switch node.Op {
	case cmp.Ceq:
		b = c == 0
	case cmp.Clt:
		b = c < 0
	case cmp.Clte:
		b = c <= 0
	case cmp.Cgt:
		b = c > 0
	case cmp.Cgte:
		b = c >= 0
	case cmp.Cne:
		b = c != 0
	default:
		panic("unreachable")
	}

	return proto.NewValueBool(b), nil
}

func (vv *valueVisitor) VisitPredicateAtom(node *ast.AtomPredicateNode) (interface{}, error) {
	return node.A.Accept(vv)
}

func (vv *valueVisitor) VisitAtomColumn(node ast.ColumnNameExpressionAtom) (interface{}, error) {
	return nil, nil
}

func (vv *valueVisitor) VisitAtomConstant(node *ast.ConstantExpressionAtom) (interface{}, error) {
	v, err := proto.NewValue(node.Value())
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	if v == nil {
		return nil, nil
	}
	return v, nil
}

func (vv *valueVisitor) VisitAtomFunction(node *ast.FunctionCallExpressionAtom) (interface{}, error) {
	return node.F.Accept(vv)
}

func (vv *valueVisitor) VisitAtomNested(node *ast.NestedExpressionAtom) (interface{}, error) {
	return node.First.Accept(vv)
}

func (vv *valueVisitor) VisitAtomUnary(node *ast.UnaryExpressionAtom) (interface{}, error) {
	prev, err := node.Inner.Accept(vv)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	var v proto.Value

	switch t := prev.(type) {
	case proto.Value:
		v = t
	}
	if v == nil {
		return nil, nil
	}

	switch node.Operator {
	case "-":
		d, err := v.Decimal()
		if err != nil {
			d = decimal.Zero
		}
		d = d.Mul(decimal.NewFromInt(-1))
		return proto.NewValueDecimal(d), nil
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

	var (
		x, y decimal.NullDecimal
		z    decimal.Decimal
	)
	switch v := l.(type) {
	case proto.Value:
		d, err := v.Decimal()
		if err != nil {
			d = decimal.Zero
		}
		x.Valid = true
		x.Decimal = d
	}
	switch v := r.(type) {
	case proto.Value:
		d, err := v.Decimal()
		if err != nil {
			d = decimal.Zero
		}
		y.Valid = true
		y.Decimal = d
	}

	if !x.Valid || !y.Valid {
		return nil, nil
	}

	switch node.Operator {
	case opcode.Plus.Literal():
		z = x.Decimal.Add(y.Decimal)
	case opcode.Minus.Literal():
		z = x.Decimal.Sub(y.Decimal)
	case opcode.Mul.Literal():
		z = x.Decimal.Mul(y.Decimal)
	case opcode.Div.Literal():
		if y.Decimal.IsZero() {
			return nil, nil
		}
		z = x.Decimal.Div(y.Decimal)
	case opcode.IntDiv.Literal():
		if y.Decimal.IsZero() {
			return nil, nil
		}
		z = x.Decimal.Div(y.Decimal).Floor()
	default:
		// TODO: need implementation
		return nil, perrors.Errorf("unsupported math opcode '%s'", node.Operator)
	}
	return proto.NewValueDecimal(z), nil
}

func (vv *valueVisitor) VisitAtomSystemVariable(node *ast.SystemVariableExpressionAtom) (interface{}, error) {
	return nil, errNotValue
}

func (vv *valueVisitor) VisitAtomVariable(node ast.VariableExpressionAtom) (interface{}, error) {
	return vv.args[node.N()], nil
}

func (vv *valueVisitor) VisitAtomInterval(node *ast.IntervalExpressionAtom) (interface{}, error) {
	// TODO implement me
	panic("implement me")
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
			ret, err := next.Accept(vv)
			if err != nil {
				return nil, perrors.WithStack(err)
			}
			switch t := ret.(type) {
			case proto.Value:
				return t, nil
			default:
				return nil, nil
			}
		}))
	}

	res, err := fn.Apply(context.Background(), args...)
	if err != nil {
		return nil, perrors.Wrapf(err, "failed to call function '%s'", node.Name())
	}

	return res, nil
}

func (vv *valueVisitor) VisitFunctionCast(node *ast.CastFunction) (interface{}, error) {
	var (
		castFuncName string
		ok           bool
		cast         *ast.ConvertDataType
	)

	var args []proto.Valuer
	args = append(args, proto.FuncValuer(func(ctx context.Context) (proto.Value, error) {
		ret, err := node.Source().Accept(vv)
		if err != nil {
			return nil, perrors.WithStack(err)
		}
		switch t := ret.(type) {
		case proto.Value:
			return t, nil
		default:
			return nil, nil
		}
	}))

	if cast, ok = node.GetCast(); ok {
		var charset string
		if charset, ok = cast.Charset(); ok {
			castFuncName = "CAST_CHARSET"
		} else {
			switch cast.Type() {
			case ast.CastToSigned, ast.CastToSignedInteger:
				castFuncName = "CAST_SIGNED"
			case ast.CastToUnsigned, ast.CastToUnsignedInteger:
				castFuncName = "CAST_UNSIGNED"
			case ast.CastToDecimal:
				castFuncName = "CAST_DECIMAL"
			case ast.CastToChar:
				castFuncName = "CAST_CHAR"
			case ast.CastToNChar:
				castFuncName = "CAST_NCHAR"
			case ast.CastToBinary:
				castFuncName = "CAST_BINARY"
			case ast.CastToDate:
				castFuncName = "CAST_DATE"
			case ast.CastToDateTime:
				castFuncName = "CAST_DATETIME"
			case ast.CastToTime:
				castFuncName = "CAST_TIME"
			case ast.CastToJson:
				castFuncName = "CAST_JSON"
			default:
				return nil, perrors.Errorf("unknown CAST type %v!", cast.Type())
			}
		}

		if len(charset) > 0 {
			args = append(args, proto.ToValuer(proto.NewValueString(charset)))
		} else {
			first, second := cast.Dimensions()
			args = append(
				args,
				proto.ToValuer(proto.NewValueInt64(first)),
				proto.ToValuer(proto.NewValueInt64(second)),
			)
		}
	}

	castFunc, ok := proto.GetFunc(castFuncName)
	if !ok {
		return nil, perrors.Errorf("no such built-in mysql function '%s'", castFuncName)
	}

	if minimum := castFunc.NumInput(); len(args) < minimum {
		return nil, perrors.Errorf("not enough function args length: minimum=%d, actual=%d", minimum, len(args))
	}

	ret, err := castFunc.Apply(context.Background(), args...)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	switch t := ret.(type) {
	case proto.Value:
		return t, nil
	default:
		return nil, nil
	}
}

func (vv *valueVisitor) VisitFunctionCaseWhenElse(node *ast.CaseWhenElseFunction) (interface{}, error) {
	var (
		caseValue proto.Value
		whenValue proto.Value
	)
	if c := node.CaseBlock; c != nil {
		v, err := c.Accept(vv)
		if err != nil {
			return nil, perrors.Wrap(err, "cannot eval value of CASE")
		}
		switch t := v.(type) {
		case proto.Value:
			caseValue = t
		}
	}

	for _, b := range node.BranchBlocks {
		v, err := b.When.Accept(vv)
		if err != nil {
			return nil, perrors.WithStack(err)
		}
		switch t := v.(type) {
		case proto.Value:
			whenValue = t
		}

		// 1. CASE WHEN x>=60 then 'PASS' ...
		// 2. CASE x WHEN 2 then 'OK' ...
		if caseValue == nil && whenValue != nil {
			matched, err := whenValue.Bool()
			if err != nil {
				return nil, perrors.WithStack(err)
			}
			if matched {
				return b.Then.Accept(vv)
			}
		}

		if caseValue != nil && whenValue != nil && caseValue.String() == whenValue.String() {
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
		v, err := proto.NewValue(node.Value)
		if err != nil {
			return nil, perrors.WithStack(err)
		}
		if v == nil {
			return nil, nil
		}
		return v, nil
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
