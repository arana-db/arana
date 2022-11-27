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
	"context"
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

import (
	"github.com/arana-db/parser/opcode"

	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
	"github.com/arana-db/arana/pkg/util/math"
)

var _ proto.Plan = (*MappingPlan)(nil)

// MappingPlan represents a query plan which will mapping column values.
type MappingPlan struct {
	proto.Plan
	Fields []ast.SelectElement
}

func (mp *MappingPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	res, err := mp.Plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mappings := mp.probe()

	transform := func(row proto.Row) (proto.Row, error) {
		inputs := make([]proto.Value, len(fields))
		if err = row.Scan(inputs); err != nil {
			return nil, errors.WithStack(err)
		}

		m := make(map[string]proto.Value)
		for i := range inputs {
			m[fields[i].Name()] = inputs[i]
		}

		var (
			next proto.Value
			vt   virtualValueVisitor
		)
		vt.row = m

		for k := range mappings {
			if next, err = mappings[k].Mapping.Accept(&vt); err != nil {
				return nil, errors.WithStack(err)
			}
			inputs[k] = next
		}

		if row.IsBinary() {
			return rows.NewBinaryVirtualRow(fields, inputs), nil
		}

		return rows.NewTextVirtualRow(fields, inputs), nil
	}

	nextDs := dataset.Pipe(ds, dataset.Map(nil, transform))
	return resultx.New(resultx.WithDataset(nextDs)), nil
}

func (mp *MappingPlan) probe() map[int]*ext.MappingSelectElement {
	mappings := make(map[int]*ext.MappingSelectElement)
	for i := range mp.Fields {
		switch field := mp.Fields[i].(type) {
		case *ext.MappingSelectElement:
			mappings[i] = field
		case *ext.WeakSelectElement:
			switch next := field.SelectElement.(type) {
			case *ext.MappingSelectElement:
				mappings[i] = next
			}
		}
	}
	return mappings
}

type virtualValueVisitor struct {
	ast.BaseVisitor
	row map[string]proto.Value
}

func (vt *virtualValueVisitor) VisitSelectElementFunction(node *ast.SelectElementFunction) (interface{}, error) {
	switch f := node.Function().(type) {
	case *ast.Function, *ast.CaseWhenElseFunction:
		res, err := f.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return res, nil
	}
	// TODO: need implementation
	panic(fmt.Sprintf("implement me: %T.VisitSelectElementFunction!", vt))
}

func (vt *virtualValueVisitor) VisitSelectElementExpr(node *ast.SelectElementExpr) (interface{}, error) {
	return node.Expression().Accept(vt)
}

func (vt *virtualValueVisitor) VisitPredicateExpression(node *ast.PredicateExpressionNode) (interface{}, error) {
	return node.P.Accept(vt)
}

func (vt *virtualValueVisitor) VisitPredicateAtom(node *ast.AtomPredicateNode) (interface{}, error) {
	return node.A.Accept(vt)
}

func (vt *virtualValueVisitor) VisitAtomColumn(node ast.ColumnNameExpressionAtom) (interface{}, error) {
	suffix := node.Suffix()
	value, ok := vt.row[suffix]
	if !ok {
		return nil, errors.Errorf("no such column '%s' found", suffix)
	}
	return value, nil
}

func (vt *virtualValueVisitor) VisitAtomConstant(node *ast.ConstantExpressionAtom) (interface{}, error) {
	return node.Value(), nil
}

func (vt *virtualValueVisitor) VisitAtomFunction(node *ast.FunctionCallExpressionAtom) (interface{}, error) {
	switch f := node.F.(type) {
	case *ast.AggrFunction:
		var sb strings.Builder
		_ = f.Restore(0, &sb, nil)
		name := sb.String()
		value, ok := vt.row[name]
		if !ok {
			return nil, errors.Errorf("no such column '%s'", name)
		}
		return value, nil
	case *ast.Function:
		value, err := f.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return value, nil
	default:
		return nil, errors.Errorf("todo: %T is not supported yet", f)
	}
}

func (vt *virtualValueVisitor) VisitAtomUnary(node *ast.UnaryExpressionAtom) (interface{}, error) {
	val, err := node.Inner.Accept(vt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	switch strings.ToLower(strings.TrimSpace(node.Operator)) {
	case "not":
		if strings.TrimFunc(proto.PrintValue(val), func(r rune) bool {
			return unicode.IsSpace(r) || r == '0'
		}) == "" {
			return int64(1), nil
		}
		return int64(0), nil
	case "-":
		switch v := val.(type) {
		case int:
			return -1 * v, nil
		case int64:
			return -1 * v, nil
		case int32:
			return -1 * v, nil
		case int16:
			return -1 * v, nil
		case int8:
			return -1 * v, nil
		case float32:
			return -1 * v, nil
		case float64:
			return -1 * v, nil
		case *gxbig.Decimal:
			d, _ := decimal.NewFromString(v.String())
			d = d.Mul(decimal.NewFromInt(-1))
			ret, _ := gxbig.NewDecFromString(d.String())
			return ret, nil
		default:
			panic(fmt.Sprintf("todo: unary for %T!", v))
		}
	}

	panic(fmt.Sprintf("todo: unary operator %s", node.Operator))
}

func (vt *virtualValueVisitor) VisitFunctionCaseWhenElse(node *ast.CaseWhenElseFunction) (interface{}, error) {
	var (
		caseValue    proto.Value
		hasCaseValue bool
	)
	if c := node.CaseBlock; c != nil {
		hasCaseValue = true
		v, err := c.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		caseValue = v.(proto.Value)
	}

	for i := range node.BranchBlocks {
		v1, err := node.BranchBlocks[i].When.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		whenValue := v1.(proto.Value)

		v2, err := node.BranchBlocks[i].Then.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		thenValue := v2.(proto.Value)

		if hasCaseValue && (whenValue == caseValue || proto.PrintValue(whenValue) == proto.PrintValue(caseValue)) {
			return thenValue, nil
		}

		if !hasCaseValue && reflect.ValueOf(whenValue).IsValid() {
			return thenValue, nil
		}
	}

	if node.ElseBlock != nil {
		elseValue, err := node.ElseBlock.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return elseValue.(proto.Value), nil
	}

	return nil, nil
}

func (vt *virtualValueVisitor) VisitFunctionArg(arg *ast.FunctionArg) (interface{}, error) {
	var (
		next interface{}
		err  error
	)
	switch arg.Type {
	case ast.FunctionArgColumn:
		next, err = arg.Value.(ast.ColumnNameExpressionAtom).Accept(vt)
	case ast.FunctionArgExpression:
		next, err = arg.Value.(ast.ExpressionNode).Accept(vt)
	case ast.FunctionArgConstant:
		next = arg.Value
	case ast.FunctionArgFunction:
		next, err = arg.Value.(*ast.Function).Accept(vt)
	case ast.FunctionArgAggrFunction:
		next, err = arg.Value.(*ast.AggrFunction).Accept(vt)
	case ast.FunctionArgCaseWhenElseFunction:
		next, err = arg.Value.(*ast.CaseWhenElseFunction).Accept(vt)
	case ast.FunctionArgCastFunction:
		next, err = arg.Value.(*ast.CastFunction).Accept(vt)
	default:
		panic("unreachable")
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}
	return next.(proto.Value), nil
}

func (vt *virtualValueVisitor) VisitFunction(node *ast.Function) (interface{}, error) {
	getValuers := func(args []*ast.FunctionArg) []proto.Valuer {
		valuers := make([]proto.Valuer, 0, len(args))
		for i := range args {
			arg := args[i]
			valuer := func(ctx context.Context) (proto.Value, error) {
				next, err := arg.Accept(vt)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				return next.(proto.Value), nil
			}
			valuers = append(valuers, proto.FuncValuer(valuer))
		}
		return valuers
	}

	funcName := node.Name()
	nextFunc, ok := proto.GetFunc(funcName)
	if !ok {
		return nil, errors.Errorf("no such mysql function '%s'", funcName)
	}
	args := getValuers(node.Args())
	if len(args) < nextFunc.NumInput() {
		return nil, errors.Errorf("incorrect parameter count in the call to native function '%s'", funcName)
	}
	res, err := nextFunc.Apply(context.Background(), args...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to call function '%s'", funcName)
	}
	return res, nil
}

func (vt *virtualValueVisitor) VisitAtomNested(node *ast.NestedExpressionAtom) (interface{}, error) {
	return node.First.Accept(vt)
}

func (vt *virtualValueVisitor) VisitAtomMath(node *ast.MathExpressionAtom) (interface{}, error) {
	toDecimal := func(atom ast.ExpressionAtom) (*gxbig.Decimal, error) {
		res, err := atom.Accept(vt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return math.ToDecimal(res), nil
	}

	var (
		result      gxbig.Decimal
		left, right *gxbig.Decimal
		err         error
	)

	if left, err = toDecimal(node.Left); err != nil {
		return nil, errors.WithStack(err)
	}
	if right, err = toDecimal(node.Right); err != nil {
		return nil, errors.WithStack(err)
	}

	switch node.Operator {
	case opcode.Plus.Literal():
		err = gxbig.DecimalAdd(left, right, &result)
	case opcode.Minus.Literal():
		err = gxbig.DecimalSub(left, right, &result)
	case opcode.Mul.Literal():
		err = gxbig.DecimalAdd(left, right, &result)
	case opcode.Div.Literal():
		if err = gxbig.DecimalDiv(left, right, &result, 4); errors.Is(err, gxbig.ErrDivByZero) {
			return nil, nil
		}
	case opcode.IntDiv.Literal():
		err = gxbig.DecimalDiv(left, right, &result, 4)
		if errors.Is(err, gxbig.ErrDivByZero) {
			return nil, nil
		}
		if err == nil {
			n, _ := result.ToInt()
			result = *gxbig.NewDecFromInt(n)
		}
	default:
		panic("implement me")
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return result.String(), nil
}
