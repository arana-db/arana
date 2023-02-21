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
)

import (
	"github.com/arana-db/parser/opcode"

	"github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

import (
	mConstants "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/dataset"
	mysqlErrors "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
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
		vt.Context = ctx
		vt.row = m

		for k := range mappings {
			var vv interface{}
			if vv, err = mappings[k].Mapping.Accept(&vt); err != nil {
				return nil, errors.WithStack(err)
			}
			next = vv.(proto.Value)

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
	context.Context
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
	v, err := proto.NewValue(node.Value())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if v == nil {
		return nil, nil
	}
	return v, nil
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
	prev, err := node.Inner.Accept(vt)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if prev == nil {
		return nil, nil
	}

	val := prev.(proto.Value)

	switch strings.ToLower(strings.TrimSpace(node.Operator)) {
	case "not":
		if b, _ := val.Bool(); b {
			return proto.NewValueInt64(0), nil
		}
		return proto.NewValueInt64(1), nil
	case "-":
		d, err := val.Decimal()
		if err != nil {
			d = decimal.Zero
		}
		return proto.NewValueDecimal(d.Mul(decimal.NewFromInt(-1))), nil
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

		if hasCaseValue && (whenValue == caseValue || whenValue.String() == caseValue.String()) {
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
		var v proto.Value
		if v, err = proto.NewValue(arg.Value); err != nil {
			break
		}
		next = v
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

	switch t := next.(type) {
	case proto.Value:
		if t == nil {
			return nil, nil
		}
		return t, nil
	default:
		return nil, nil
	}
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
				switch val := next.(type) {
				case proto.Value:
					return val, nil
				default:
					return nil, nil
				}
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

	if !proto.ValidateFunction(vt, nextFunc, false) {
		schema, _ := vt.Context.Value(proto.ContextKeySchema{}).(string)
		var sb strings.Builder
		sb.Grow(32 + len(schema) + len(funcName))

		sb.WriteString("FUNCTION ")
		if len(schema) > 0 {
			sb.WriteString(schema)
			sb.WriteByte('.')
		}
		sb.WriteString(funcName)
		sb.WriteString(" doesn't exist")

		return nil, mysqlErrors.NewSQLError(
			mConstants.ERSPDoseNotExist,
			mConstants.SS42000,
			sb.String(),
		)
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
	toDecimal := func(atom ast.ExpressionAtom) (decimal.NullDecimal, error) {
		var (
			d   decimal.Decimal
			res interface{}
			ret decimal.NullDecimal
			err error
			v   proto.Value
			ok  bool
		)

		if res, err = atom.Accept(vt); err != nil {
			return ret, errors.WithStack(err)
		}

		if v, ok = res.(proto.Value); !ok || v == nil {
			return ret, nil
		}

		if d, err = v.Decimal(); err != nil {
			return ret, errors.WithStack(err)
		}
		ret.Valid = true
		ret.Decimal = d

		return ret, nil
	}

	var (
		result      decimal.Decimal
		left, right decimal.NullDecimal
		err         error
	)

	if left, err = toDecimal(node.Left); err != nil {
		return nil, errors.WithStack(err)
	}
	if right, err = toDecimal(node.Right); err != nil {
		return nil, errors.WithStack(err)
	}

	if !left.Valid || !right.Valid {
		return nil, nil
	}

	switch node.Operator {
	case opcode.Plus.Literal():
		result = left.Decimal.Add(right.Decimal)
	case opcode.Minus.Literal():
		result = left.Decimal.Sub(right.Decimal)
	case opcode.Mul.Literal():
		result = left.Decimal.Mul(right.Decimal)
	case opcode.Div.Literal():
		if right.Decimal.IsZero() {
			return nil, nil
		}
		result = left.Decimal.Div(right.Decimal)
	case opcode.IntDiv.Literal():
		if right.Decimal.IsZero() {
			return nil, nil
		}
		result = left.Decimal.Div(right.Decimal).Floor()
	default:
		return nil, errors.Errorf("unsupported math operator %s", node.Operator)
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return proto.NewValueDecimal(result), nil
}
