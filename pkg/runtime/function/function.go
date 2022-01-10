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

package function

import (
	stdErrors "errors"
	"strconv"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/cmp"
	"github.com/dubbogo/arana/pkg/runtime/misc"
	"github.com/dubbogo/arana/pkg/runtime/xxast"
)

const _prefixMySQLFunc = "$"

var ErrCannotEvalWithColumnName = stdErrors.New("cannot eval function with column name")

var globalCalculator calculator

func IsEvalWithColumnErr(err error) bool {
	return err == ErrCannotEvalWithColumnName
}

// TranslateFunction translates the given function to internal function name.
func TranslateFunction(name string) string {
	return _prefixMySQLFunc + name
}

func EvalCastFunction(node *xxast.CastFunction, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildCastFunction(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

func EvalCaseWhenFunction(node *xxast.CaseWhenElseFunction, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildCaseWhenFunction(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

// EvalFunction calculates the result of math expression with custom args.
func EvalFunction(node *xxast.Function, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildFunction(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

// Eval calculates the result of math expression with custom args.
func Eval(node *xxast.MathExpressionAtom, args ...interface{}) (interface{}, error) {
	s, err := globalCalculator.buildMath(node)
	if err != nil {
		return nil, err
	}
	return EvalString(s, args...)
}

// EvalString computes the result of given expression script with custom args.
func EvalString(script string, args ...interface{}) (interface{}, error) {
	vm := BorrowVM()
	defer ReturnVM(vm)
	return vm.Eval(script, args)
}

type calculator struct {
}

func (c *calculator) buildCaseWhenFunction(node *xxast.CaseWhenElseFunction) (string, error) {
	var sb strings.Builder
	if err := caseWhenFunction2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (c *calculator) buildCastFunction(node *xxast.CastFunction) (string, error) {
	var sb strings.Builder
	if err := castFunction2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (c *calculator) buildFunction(node *xxast.Function) (string, error) {
	var sb strings.Builder
	if err := function2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (c *calculator) buildMath(node *xxast.MathExpressionAtom) (string, error) {
	var sb strings.Builder
	if err := math2script(&sb, node); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func exprAtom2script(sb *strings.Builder, node xxast.ExpressionAtom) error {
	switch v := node.(type) {
	case *xxast.MathExpressionAtom:
		if err := math2script(sb, v); err != nil {
			return err
		}
	case *xxast.ConstantExpressionAtom:
		sb.WriteString(v.String())
	case *xxast.UnaryExpressionAtom:
		sb.WriteString(FuncUnary)
		sb.WriteString("('")
		sb.WriteString(v.Operator)
		sb.WriteString("', ")
		if err := exprAtom2script(sb, v.Inner); err != nil {
			return err
		}
		sb.WriteByte(')')
	case xxast.ColumnNameExpressionAtom:
		return ErrCannotEvalWithColumnName
	case *xxast.NestedExpressionAtom:
		next := v.First.(*xxast.PredicateExpressionNode).P.(*xxast.AtomPredicateNode).A
		sb.WriteByte('(')
		if err := exprAtom2script(sb, next); err != nil {
			return err
		}
		sb.WriteByte(')')
	case xxast.VariableExpressionAtom:
		writeVariable(sb, v.N())
	case *xxast.FunctionCallExpressionAtom:
		switch fn := v.F.(type) {
		case *xxast.Function:
			if err := function2script(sb, fn); err != nil {
				return err
			}
		case *xxast.AggrFunction:
			return errors.New("aggr function should not appear here")
		case *xxast.CastFunction:
			if err := castFunction2script(sb, fn); err != nil {
				return err
			}
		case *xxast.CaseWhenElseFunction:
			if err := caseWhenFunction2script(sb, fn); err != nil {
				return err
			}
		default:
			return errors.Errorf("expression atom within function call %T is not supported yet", fn)
		}
	default:
		return errors.Errorf("expression atom within %T is not supported yet", v)
	}
	return nil
}

func math2script(sb *strings.Builder, node *xxast.MathExpressionAtom) error {
	if err := exprAtom2script(sb, node.Left); err != nil {
		return err
	}

	sb.WriteByte(' ')
	sb.WriteString(node.Operator)
	sb.WriteByte(' ')

	if err := exprAtom2script(sb, node.Right); err != nil {
		return err
	}

	return nil
}

func writeVariable(sb *strings.Builder, n int) {
	sb.WriteString("arguments[")
	sb.WriteString(strconv.FormatInt(int64(n), 10))
	sb.WriteByte(']')
}

func castFunction2script(sb *strings.Builder, node *xxast.CastFunction) error {
	if cast, ok := node.GetCast(); ok {
		switch cast.Type() {
		case xxast.CastToUnsigned, xxast.CastToUnsignedInteger:
			writeFuncName(sb, "CAST_UNSIGNED")
			sb.WriteByte('(')
		case xxast.CastToSigned, xxast.CastToSignedInteger:
			writeFuncName(sb, "CAST_SIGNED")
			sb.WriteByte('(')
		case xxast.CastToBinary:
			// TODO: support binary
			return errors.New("cast to binary is not supported yet")
		case xxast.CastToNChar:
			writeFuncName(sb, "CAST_NCHAR")
			sb.WriteByte('(')
			if d, _ := cast.Dimensions(); d > 0 {
				sb.WriteString(strconv.FormatInt(d, 10))
			} else {
				sb.WriteByte('0')
			}
			sb.WriteString(", ")
		case xxast.CastToChar:
			writeFuncName(sb, "CAST_CHAR")
			sb.WriteByte('(')
			if d, _ := cast.Dimensions(); d > 0 {
				sb.WriteString(strconv.FormatInt(d, 10))
			} else {
				sb.WriteByte('0')
			}

			sb.WriteString(", ")

			if cs, ok := cast.Charset(); ok {
				sb.WriteByte('\'')
				sb.WriteString(misc.Escape(cs, misc.EscapeSingleQuote))
				sb.WriteByte('\'')
			} else {
				sb.WriteString("''")
			}

			sb.WriteString(", ")
		case xxast.CastToDate:
			writeFuncName(sb, "CAST_DATE")
			sb.WriteByte('(')
		case xxast.CastToDateTime:
			writeFuncName(sb, "CAST_DATETIME")
			sb.WriteByte('(')
		case xxast.CastToTime:
			writeFuncName(sb, "CAST_TIME")
			sb.WriteByte('(')
		case xxast.CastToJson:
			// TODO: support cast json
			return errors.New("cast to json is not supported yet")
		case xxast.CastToDecimal:
			writeFuncName(sb, "CAST_DECIMAL")
			sb.WriteByte('(')
			d0, d1 := cast.Dimensions()
			if d0 > 0 {
				sb.WriteString(strconv.FormatInt(d0, 10))
			} else {
				sb.WriteByte('0')
			}
			sb.WriteString(", ")

			if d1 > 0 {
				sb.WriteString(strconv.FormatInt(d1, 10))
			} else {
				sb.WriteByte('0')
			}
			sb.WriteString(", ")
		}
	} else if charset, ok := node.GetCharset(); ok {
		writeFuncName(sb, "CAST_CHARSET(")

		sb.WriteByte('\'')
		sb.WriteString(misc.Escape(charset, misc.EscapeSingleQuote))
		sb.WriteByte('\'')

		sb.WriteString(", ")
	} else {
		panic("unreachable")
	}

	next := node.Source().(*xxast.PredicateExpressionNode).P.(*xxast.AtomPredicateNode).A
	if err := exprAtom2script(sb, next); err != nil {
		return err
	}

	sb.WriteByte(')')

	return nil
}

func function2script(sb *strings.Builder, node *xxast.Function) error {
	writeFuncName(sb, node.Name())
	sb.WriteByte('(')
	for i, arg := range node.Args() {
		if i > 0 {
			sb.WriteByte(',')
			sb.WriteByte(' ')
		}
		if err := handleArg(sb, arg); err != nil {
			return err
		}
	}
	sb.WriteByte(')')
	return nil
}

func handleArg(sb *strings.Builder, arg *xxast.FunctionArg) error {

	handleCompareAtom := func(sb *strings.Builder, node xxast.PredicateNode) error {
		switch l := node.(type) {
		case *xxast.AtomPredicateNode:
			if err := exprAtom2script(sb, l.A); err != nil {
				return err
			}
		default:
			return errors.Errorf("unsupported compare atom node %T in case-when function", l)
		}
		return nil
	}

	switch arg.Type() {
	case xxast.FunctionArgColumn:
		return ErrCannotEvalWithColumnName
	case xxast.FunctionArgConstant:
		_ = arg.Restore(sb, nil)
	case xxast.FunctionArgExpression:
		pn := arg.Value().(*xxast.PredicateExpressionNode).P
		switch p := pn.(type) {
		case *xxast.AtomPredicateNode:
			next := p.A
			if err := exprAtom2script(sb, next); err != nil {
				return err
			}
		case *xxast.BinaryComparisonPredicateNode:
			if err := handleCompareAtom(sb, p.Left); err != nil {
				return err
			}

			sb.WriteByte(' ')

			switch p.Op {
			case cmp.Ceq:
				sb.WriteString("==")
			case cmp.Cne:
				sb.WriteString("!=")
			default:
				sb.WriteString(p.Op.String())
			}

			sb.WriteByte(' ')
			if err := handleCompareAtom(sb, p.Right); err != nil {
				return err
			}
		default:
			return errors.Errorf("unsupported %T", p)
		}

	case xxast.FunctionArgFunction:
		if err := function2script(sb, arg.Value().(*xxast.Function)); err != nil {
			return err
		}
	case xxast.FunctionArgCastFunction:
		if err := castFunction2script(sb, arg.Value().(*xxast.CastFunction)); err != nil {
			return err
		}
	case xxast.FunctionArgCaseWhenElseFunction:
		if err := caseWhenFunction2script(sb, arg.Value().(*xxast.CaseWhenElseFunction)); err != nil {
			return err
		}
	}

	return nil
}

func caseWhenFunction2script(sb *strings.Builder, node *xxast.CaseWhenElseFunction) error {
	var caseScript string

	// convert CASE header to script
	// eg: CASE 2+1 WHEN 1 THEN 'A' WHEN 2 THEN 'B' WHEN 3 THEN 'C' ELSE '*' END
	// will be converted to: $IF(1 == (2+1), 'A', $IF(2 == (2+1), 'B', $IF(3 == (2+1), 'C', '*' )))
	if c := node.Case(); c != nil {
		var b strings.Builder
		switch v := c.(type) {
		case *xxast.PredicateExpressionNode:
			switch p := v.P.(type) {
			case *xxast.AtomPredicateNode:
				if err := exprAtom2script(&b, p.A); err != nil {
					return err
				}
			default:
				return errors.Errorf("invalid expression type %T as the CASE body", v)
			}
		default:
			return errors.Errorf("invalid expression type %T as the CASE body", v)
		}
		caseScript = b.String()
	}

	for i, branch := range node.Branches() {
		var (
			when = branch[0]
			then = branch[1]
		)

		if i > 0 {
			sb.WriteString(", ")
		}

		writeFuncName(sb, "IF")

		sb.WriteByte('(')

		if err := handleArg(sb, when); err != nil {
			return err
		}

		// write CASE header
		if len(caseScript) > 0 {
			sb.WriteString(" == (")
			sb.WriteString(caseScript)
			sb.WriteByte(')')
		}

		sb.WriteString(", ")

		if err := handleArg(sb, then); err != nil {
			return err
		}
	}

	sb.WriteString(", ")

	if els, ok := node.Else(); ok {
		if err := handleArg(sb, els); err != nil {
			return err
		}
	} else {
		sb.WriteString("null")
	}

	for i := 0; i < len(node.Branches()); i++ {
		sb.WriteByte(')')
	}

	return nil
}

func writeFuncName(sb *strings.Builder, name string) {
	sb.WriteString(_prefixMySQLFunc)
	sb.WriteString(name)
}
