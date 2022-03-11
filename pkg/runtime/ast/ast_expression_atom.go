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

package ast

import (
	"fmt"
	"strconv"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/misc"
)

const (
	_ ExpressionAtomMode = iota
	EamUnary
	EamVar
	EamCol
	EamMath
	EamConst
	EamNested
	EamFunc
)

type ExpressionAtomMode uint8

var (
	_ ExpressionAtom = (ColumnNameExpressionAtom)(nil)
	_ ExpressionAtom = (*MathExpressionAtom)(nil)
	_ ExpressionAtom = (VariableExpressionAtom)(0)
	_ ExpressionAtom = (*ConstantExpressionAtom)(nil)
	_ ExpressionAtom = (*NestedExpressionAtom)(nil)
	_ ExpressionAtom = (*FunctionCallExpressionAtom)(nil)
	_ ExpressionAtom = (*UnaryExpressionAtom)(nil)
)

type ExpressionAtom interface {
	inTablesChecker
	Restorer
	Mode() ExpressionAtomMode
}

type UnaryExpressionAtom struct {
	Operator string
	Inner    ExpressionAtom
}

func (u *UnaryExpressionAtom) InTables(tables map[string]struct{}) error {
	return u.Inner.InTables(tables)
}

func (u *UnaryExpressionAtom) IsOperatorNot() bool {
	switch u.Operator {
	case "!", "NOT":
		return true
	}
	return false
}

func (u *UnaryExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString(u.Operator)
	if err := u.Inner.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (u *UnaryExpressionAtom) Mode() ExpressionAtomMode {
	return EamUnary
}

type ConstantExpressionAtom struct {
	Inner interface{}
}

func (c *ConstantExpressionAtom) String() string {
	return constant2string(c.Inner)
}

func (c *ConstantExpressionAtom) InTables(_ map[string]struct{}) error {
	return nil
}

func (c ConstantExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString(constant2string(c.Inner))
	return nil
}

func (c *ConstantExpressionAtom) Value() interface{} {
	return c.Inner
}

func (c *ConstantExpressionAtom) Mode() ExpressionAtomMode {
	return EamConst
}

func (c *ConstantExpressionAtom) CntParams() int {
	return 0
}

type ColumnNameExpressionAtom []string

func (c ColumnNameExpressionAtom) InTables(tables map[string]struct{}) error {
	if len(c) == 1 {
		return nil
	}
	if _, ok := tables[c.Prefix()]; ok {
		return nil
	}
	return errors.Errorf("unknown column '%s'", strings.Join(c, "."))
}

func (c ColumnNameExpressionAtom) Prefix() string {
	if len(c) > 1 {
		return c[0]
	}
	return ""
}

func (c ColumnNameExpressionAtom) Suffix() string {
	return c[len(c)-1]
}

func (c ColumnNameExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	misc.Wrap(sb, '`', c[0])
	for i := 1; i < len(c); i++ {
		sb.WriteByte('.')
		misc.Wrap(sb, '`', c[i])
	}
	return nil
}

func (c ColumnNameExpressionAtom) CntParams() int {
	return 0
}

func (c ColumnNameExpressionAtom) Mode() ExpressionAtomMode {
	return EamCol
}

type VariableExpressionAtom int

func (v VariableExpressionAtom) InTables(_ map[string]struct{}) error {
	return nil
}

func (v VariableExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteByte('?')
	*args = append(*args, v.N())
	return nil
}

func (v VariableExpressionAtom) N() int {
	return int(v)
}

func (v VariableExpressionAtom) CntParams() int {
	return 1
}

func (v VariableExpressionAtom) Mode() ExpressionAtomMode {
	return EamVar
}

type MathExpressionAtom struct {
	Left     ExpressionAtom
	Operator string
	Right    ExpressionAtom
}

func (m *MathExpressionAtom) InTables(tables map[string]struct{}) error {
	if err := m.Left.InTables(tables); err != nil {
		return err
	}
	if err := m.Right.InTables(tables); err != nil {
		return err
	}
	return nil
}

func (m *MathExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	if err := m.Left.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}
	misc.Wrap(sb, ' ', m.Operator)

	if err := m.Right.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *MathExpressionAtom) Mode() ExpressionAtomMode {
	return EamMath
}

type NestedExpressionAtom struct {
	First ExpressionNode
}

func (n *NestedExpressionAtom) InTables(tables map[string]struct{}) error {
	return n.First.InTables(tables)
}

func (n *NestedExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteByte('(')

	if err := n.First.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	sb.WriteByte(')')

	return nil
}

func (n *NestedExpressionAtom) Mode() ExpressionAtomMode {
	return EamNested
}

type FunctionCallExpressionAtom struct {
	F interface{} // *Function OR *AggrFunction OR *CaseWhenElseFunction OR *CastFunction
}

func (f *FunctionCallExpressionAtom) InTables(tables map[string]struct{}) error {
	if c, ok := f.F.(inTablesChecker); ok {
		if err := c.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (f *FunctionCallExpressionAtom) Restore(sb *strings.Builder, args *[]int) error {
	switch v := f.F.(type) {
	case Restorer:
		if err := v.Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
		return nil
	default:
		panic("unreachable")
	}
}

func (f *FunctionCallExpressionAtom) Mode() ExpressionAtomMode {
	return EamFunc
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
