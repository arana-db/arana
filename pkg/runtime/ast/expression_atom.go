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

package ast

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/arana-db/parser/ast"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/misc"
)

var (
	_ ExpressionAtom = (ColumnNameExpressionAtom)(nil)
	_ ExpressionAtom = (*MathExpressionAtom)(nil)
	_ ExpressionAtom = (VariableExpressionAtom)(0)
	_ ExpressionAtom = (*ConstantExpressionAtom)(nil)
	_ ExpressionAtom = (*NestedExpressionAtom)(nil)
	_ ExpressionAtom = (*FunctionCallExpressionAtom)(nil)
	_ ExpressionAtom = (*UnaryExpressionAtom)(nil)
	_ ExpressionAtom = (*SystemVariableExpressionAtom)(nil)
	_ ExpressionAtom = (*IntervalExpressionAtom)(nil)
)

type expressionAtomPhantom struct{}

type ExpressionAtom interface {
	Restorer
	paramsCounter
	phantom() expressionAtomPhantom
}

type IntervalExpressionAtom struct {
	Unit  ast.TimeUnitType
	Value PredicateNode
}

func (ie *IntervalExpressionAtom) Duration() time.Duration {
	switch ie.Unit {
	case ast.TimeUnitMicrosecond:
		return time.Microsecond
	case ast.TimeUnitSecond:
		return time.Second
	case ast.TimeUnitMinute:
		return time.Minute
	case ast.TimeUnitHour:
		return time.Hour
	case ast.TimeUnitDay:
		return time.Hour * 24
	default:
		panic(fmt.Sprintf("unsupported interval unit %s!", ie.Unit))
	}
}

func (ie *IntervalExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("INTERVAL ")
	if err := ie.Value.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	sb.WriteByte(' ')
	sb.WriteString(ie.Unit.String())

	return nil
}

func (ie *IntervalExpressionAtom) CntParams() int {
	return ie.Value.CntParams()
}

func (ie *IntervalExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type SystemVariableExpressionAtom struct {
	Name string
}

func (sy *SystemVariableExpressionAtom) Restore(_ RestoreFlag, sb *strings.Builder, _ *[]int) error {
	sb.WriteString("@@")
	sb.WriteString(sy.Name)
	return nil
}

func (sy *SystemVariableExpressionAtom) CntParams() int {
	return 0
}

func (sy *SystemVariableExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type UnaryExpressionAtom struct {
	Operator string
	Inner    interface{} // ExpressionAtom or *BinaryComparisonPredicateNode
}

func (u *UnaryExpressionAtom) IsOperatorNot() bool {
	switch u.Operator {
	case "!", "NOT":
		return true
	}
	return false
}

func (u *UnaryExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString(u.Operator)

	switch val := u.Inner.(type) {
	case ExpressionAtom:
		if err := val.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	case *BinaryComparisonPredicateNode:
		if err := val.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	default:
		panic("unreachable")
	}
	return nil
}

func (u *UnaryExpressionAtom) CntParams() int {
	switch val := u.Inner.(type) {
	case ExpressionAtom:
		return val.CntParams()
	case *BinaryComparisonPredicateNode:
		return val.CntParams()
	default:
		panic("unreachable")
	}
}

func (u *UnaryExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type ConstantExpressionAtom struct {
	Inner interface{}
}

func (c *ConstantExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString(constant2string(c.Inner))
	return nil
}

func (c *ConstantExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

func constant2string(value interface{}) string {
	switch v := value.(type) {
	case Null:
		return v.String()
	case int:
		return strconv.FormatInt(int64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case string:
		var sb strings.Builder
		sb.Grow(len(v) + 16)
		sb.WriteByte('\'')
		misc.WriteEscape(&sb, v, misc.EscapeSingleQuote)
		sb.WriteByte('\'')
		return sb.String()
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	default:
		panic(fmt.Sprintf("todo: render %T to string!", v))
	}
}

func (c *ConstantExpressionAtom) String() string {
	return constant2string(c.Inner)
}

func (c *ConstantExpressionAtom) Value() interface{} {
	return c.Inner
}

func (c *ConstantExpressionAtom) CntParams() int {
	return 0
}

type ColumnNameExpressionAtom []string

func (c ColumnNameExpressionAtom) Prefix() string {
	if len(c) > 1 {
		return c[0]
	}
	return ""
}

func (c ColumnNameExpressionAtom) Suffix() string {
	return c[len(c)-1]
}

func (c ColumnNameExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, _ *[]int) error {
	WriteID(sb, c[0])

	for i := 1; i < len(c); i++ {
		sb.WriteByte('.')
		WriteID(sb, c[i])
	}
	return nil
}

func (c ColumnNameExpressionAtom) String() string {
	return MustRestoreToString(RestoreDefault, c)
}

func (c ColumnNameExpressionAtom) CntParams() int {
	return 0
}

func (c ColumnNameExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type VariableExpressionAtom int

func (v VariableExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteByte('?')

	if args != nil {
		*args = append(*args, v.N())
	}

	return nil
}

func (v VariableExpressionAtom) N() int {
	return int(v)
}

func (v VariableExpressionAtom) CntParams() int {
	return 1
}

func (v VariableExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type MathExpressionAtom struct {
	Left     ExpressionAtom
	Operator string
	Right    ExpressionAtom
}

func (m *MathExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := m.Left.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	sb.WriteString(m.Operator)
	if err := m.Right.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *MathExpressionAtom) CntParams() int {
	return m.Left.CntParams() + m.Right.CntParams()
}

func (m *MathExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type NestedExpressionAtom struct {
	First ExpressionNode
}

func (n *NestedExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteByte('(')
	if err := n.First.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	sb.WriteByte(')')

	return nil
}

func (n *NestedExpressionAtom) CntParams() (ret int) {
	return n.First.CntParams()
}

func (n *NestedExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}

type FunctionCallExpressionAtom struct {
	F interface{} // *Function OR *AggrFunction OR *CaseWhenElseFunction OR *CastFunction
}

func (f *FunctionCallExpressionAtom) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	var err error
	switch v := f.F.(type) {
	case *Function:
		err = v.Restore(flag, sb, args)
	case *AggrFunction:
		err = v.Restore(flag, sb, args)
	case *CaseWhenElseFunction:
		err = v.Restore(flag, sb, args)
	case *CastFunction:
		err = v.Restore(flag, sb, args)
	default:
		panic("unreachable")
	}

	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (f *FunctionCallExpressionAtom) CntParams() int {
	switch v := f.F.(type) {
	case *Function:
		return v.CntParams()
	case *AggrFunction:
		return 0
	case *CastFunction:
		return v.CntParams()
	case *CaseWhenElseFunction:
		return v.CntParams()
	default:
		panic("unreachable")
	}
}

func (f *FunctionCallExpressionAtom) phantom() expressionAtomPhantom {
	return expressionAtomPhantom{}
}
