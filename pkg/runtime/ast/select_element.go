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
	"strings"
)

import (
	"github.com/pkg/errors"
)

var (
	_ SelectElement = (*SelectElementAll)(nil)
	_ SelectElement = (*SelectElementExpr)(nil)
	_ SelectElement = (*SelectElementFunction)(nil)
	_ SelectElement = (*SelectElementColumn)(nil)
)

type selectElementPhantom struct{}

// SelectElement represents a select element.
type SelectElement interface {
	Node
	Restorer

	// Alias returns the alias if available.
	Alias() string

	// ToSelectString converts current SelectElement to SQL SELECT string and return it.
	ToSelectString() string

	// DisplayName returns the actual display name of field.
	DisplayName() string

	phantom() selectElementPhantom
}

type SelectElementAll struct {
	prefix string
}

func (s *SelectElementAll) DisplayName() string {
	return s.ToSelectString()
}

func (s *SelectElementAll) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitSelectElementWildcard(s)
}

func (s *SelectElementAll) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if prefix := s.Prefix(); len(prefix) > 0 {
		sb.WriteString(prefix)
		sb.WriteByte('.')
	}
	sb.WriteByte('*')
	return nil
}

func (s *SelectElementAll) ToSelectString() string {
	var sb strings.Builder
	if len(s.prefix) > 0 {
		WriteID(&sb, s.prefix)
		sb.WriteByte('.')
	}
	sb.WriteByte('*')
	return sb.String()
}

func (s *SelectElementAll) Alias() string {
	return ""
}

func (s *SelectElementAll) Prefix() string {
	return s.prefix
}

func (s *SelectElementAll) phantom() selectElementPhantom {
	return selectElementPhantom{}
}

type SelectElementExpr struct {
	inner        ExpressionNode
	alias        string
	originalText string
}

func (s *SelectElementExpr) DisplayName() string {
	if len(s.alias) > 0 {
		return s.alias
	}
	if len(s.originalText) > 0 {
		return s.originalText
	}
	return s.ToSelectString()
}

func (s *SelectElementExpr) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitSelectElementExpr(s)
}

func (s *SelectElementExpr) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := s.Expression().Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if len(s.alias) > 0 && !flag.Has(RestoreWithoutAlias) {
		sb.WriteString(" AS ")
		WriteID(sb, s.alias)
	}

	return nil
}

func (s *SelectElementExpr) ToSelectString() string {
	return MustRestoreToString(RestoreDefault, s.Expression())
}

func (s *SelectElementExpr) Expression() ExpressionNode {
	return s.inner
}

func (s *SelectElementExpr) Alias() string {
	return s.alias
}

func (s *SelectElementExpr) phantom() selectElementPhantom {
	return selectElementPhantom{}
}

type SelectElementFunction struct {
	inner        Node // *Function or *AggrFunction
	alias        string
	originalText string
}

func (s *SelectElementFunction) DisplayName() string {
	if len(s.alias) > 0 {
		return s.alias
	}
	if len(s.originalText) > 0 {
		return s.originalText
	}
	return s.ToSelectString()
}

func (s *SelectElementFunction) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitSelectElementFunction(s)
}

func (s *SelectElementFunction) phantom() selectElementPhantom {
	return selectElementPhantom{}
}

func (s *SelectElementFunction) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	var err error
	switch fn := s.Function().(type) {
	case *Function:
		err = fn.Restore(flag, sb, args)
	case *AggrFunction:
		err = fn.Restore(flag, sb, args)
	case *CastFunction:
		err = fn.Restore(flag, sb, args)
	case *CaseWhenElseFunction:
		err = fn.Restore(flag, sb, args)
	default:
		panic("unreachable")
	}
	if err != nil {
		return errors.WithStack(err)
	}

	if len(s.alias) > 0 && !flag.Has(RestoreWithoutAlias) {
		sb.WriteString(" AS ")
		WriteID(sb, s.alias)
	}

	return nil
}

func (s *SelectElementFunction) ToSelectString() string {
	var (
		sb  strings.Builder
		err error
	)
	switch fun := s.inner.(type) {
	case *Function:
		err = fun.Restore(RestoreDefault, &sb, nil)
	case *AggrFunction:
		err = fun.Restore(RestoreDefault, &sb, nil)
	case *CastFunction:
		err = fun.Restore(RestoreDefault, &sb, nil)
	case *CaseWhenElseFunction:
		err = fun.Restore(RestoreDefault, &sb, nil)
	default:
		panic("unreachable")
	}
	if err != nil {
		panic(err.Error())
	}
	return sb.String()
}

func (s *SelectElementFunction) SetCaseWhenElseFunction(fn *CaseWhenElseFunction) {
	s.inner = fn
}

func (s *SelectElementFunction) SetCastFunction(fn *CastFunction) {
	s.inner = fn
}

func (s *SelectElementFunction) SetFunction(fn *Function) {
	s.inner = fn
}

func (s *SelectElementFunction) SetAggrFunction(fn *AggrFunction) {
	s.inner = fn
}

func (s *SelectElementFunction) SetAlias(alias string) {
	s.alias = alias
}

func (s *SelectElementFunction) Function() Node {
	return s.inner
}

func (s *SelectElementFunction) Alias() string {
	return s.alias
}

type SelectElementColumn struct {
	Name  []string
	alias string
}

func (s *SelectElementColumn) DisplayName() string {
	if len(s.alias) > 0 {
		return s.alias
	}
	return s.Suffix()
}

func (s *SelectElementColumn) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitSelectElementColumn(s)
}

func (s *SelectElementColumn) Suffix() string {
	if len(s.Name) < 1 {
		return ""
	}
	return s.Name[len(s.Name)-1]
}

func (s *SelectElementColumn) Prefix() string {
	if len(s.Name) < 2 {
		return ""
	}
	return s.Name[len(s.Name)-2]
}

func (s *SelectElementColumn) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := ColumnNameExpressionAtom(s.Name).Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if len(s.alias) > 0 && !flag.Has(RestoreWithoutAlias) {
		sb.WriteString(" AS ")
		WriteID(sb, s.alias)
	}
	return nil
}

func (s *SelectElementColumn) ToSelectString() string {
	return ColumnNameExpressionAtom(s.Name).String()
}

func (s *SelectElementColumn) Alias() string {
	return s.alias
}

func (s *SelectElementColumn) phantom() selectElementPhantom {
	return selectElementPhantom{}
}

func NewSelectElementColumn(name []string, alias string) *SelectElementColumn {
	return &SelectElementColumn{
		Name:  name,
		alias: alias,
	}
}

func NewSelectElementExpr(expr ExpressionNode, alias string) *SelectElementExpr {
	return &SelectElementExpr{
		inner: expr,
		alias: alias,
	}
}

func NewSelectElementExprFull(expr ExpressionNode, alias string, originalText string) *SelectElementExpr {
	return &SelectElementExpr{
		inner:        expr,
		alias:        alias,
		originalText: originalText,
	}
}

func NewSelectElementFunction(fun *Function, alias string) *SelectElementFunction {
	return &SelectElementFunction{
		inner: fun,
		alias: alias,
	}
}

func NewSelectElementAggrFunction(fun *AggrFunction, alias string) *SelectElementFunction {
	return &SelectElementFunction{
		inner: fun,
		alias: alias,
	}
}

func NewSelectElementCastFunction(fun *CastFunction, alias string) *SelectElementFunction {
	return &SelectElementFunction{
		inner: fun,
		alias: alias,
	}
}

func NewSelectElementCaseWhenFunction(fun *CaseWhenElseFunction, alias string) *SelectElementFunction {
	return &SelectElementFunction{
		inner: fun,
		alias: alias,
	}
}
