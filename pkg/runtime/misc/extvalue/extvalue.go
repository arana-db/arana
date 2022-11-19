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
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/js"
	"github.com/arana-db/arana/pkg/runtime/misc"
)

func GetValue(next ast.PredicateNode, args []interface{}) (interface{}, error) {
	switch v := next.(type) {
	case *ast.AtomPredicateNode:
		return GetValueFromAtom(v.A, args)
	}
	return nil, errors.Errorf("get value from %T is not supported yet", next)
}

func GetValueFromAtom(atom ast.ExpressionAtom, args []interface{}) (interface{}, error) {
	switch it := atom.(type) {
	case *ast.UnaryExpressionAtom:
		var (
			v   interface{}
			err error
		)
		switch inner := it.Inner.(type) {
		case ast.ExpressionAtom:
			v, err = GetValueFromAtom(inner, args)
		case *ast.BinaryComparisonPredicateNode:
			v, err = GetValue(inner, args)
		default:
			panic("unreachable")
		}

		if err != nil {
			return nil, err
		}
		return misc.ComputeUnary(it.Operator, v)
	case *ast.ConstantExpressionAtom:
		return it.Value(), nil
	case ast.VariableExpressionAtom:
		return args[it.N()], nil
	case *ast.MathExpressionAtom:
		return js.Eval(it, args...)
	case *ast.FunctionCallExpressionAtom:
		switch fn := it.F.(type) {
		case *ast.Function:
			return js.EvalFunction(fn, args...)
		case *ast.AggrFunction:
			return nil, errors.New("aggregate function should not appear here")
		case *ast.CastFunction:
			return js.EvalCastFunction(fn, args...)
		case *ast.CaseWhenElseFunction:
			return js.EvalCaseWhenFunction(fn, args...)
		default:
			return nil, errors.Errorf("get value from %T is not supported yet", it)
		}
	case ast.ColumnNameExpressionAtom:
		return nil, js.ErrCannotEvalWithColumnName
	case *ast.NestedExpressionAtom:
		nested, ok := it.First.(*ast.PredicateExpressionNode)
		if !ok {
			return nil, errors.Errorf("only those nest expressions within predicated expression node is supported")
		}
		return GetValue(nested.P, args)
	default:
		return nil, errors.Errorf("extracting value from %T is not supported yet", it)
	}
}
