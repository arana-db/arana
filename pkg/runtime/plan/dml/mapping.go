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
	"strings"
)

import (
	"github.com/arana-db/parser/opcode"

	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
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
	default:
		return nil, errors.Errorf("todo: %T is not supported yet", f)
	}
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
		switch val := res.(type) {
		case string:
			return gxbig.NewDecFromString(val)
		case *gxbig.Decimal:
			return val, nil
		case float64:
			return gxbig.NewDecFromFloat(val)
		case float32:
			return gxbig.NewDecFromFloat(float64(val))
		case int64:
			return gxbig.NewDecFromInt(val), nil
		default:
			return gxbig.NewDecFromString(fmt.Sprint(val))
		}
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
	default:
		panic("implement me")
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return result.String(), nil
}
