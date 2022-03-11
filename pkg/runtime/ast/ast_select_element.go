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
	"strings"
)

import (
	"github.com/pkg/errors"
)

type SelMode uint8

const (
	_ SelMode = iota
	SelAll
	SelCol
	SelFunc
	SelExpr
)

var (
	_ SelectElement = (*SelectElementAll)(nil)
	_ SelectElement = (*SelectElementExpr)(nil)
	_ SelectElement = (*SelectElementFunction)(nil)
	_ SelectElement = (*SelectElementColumn)(nil)
)

// SelectElement represents a select element.
type SelectElement interface {
	inTablesChecker
	// Alias returns the alias if available.
	Alias() string
	// Mode returns the SelMode.
	Mode() SelMode
	// ToSelectString converts current SelectElement to SQL SELECT string and return it.
	ToSelectString() string
}

type SelectElementAll struct {
	prefix string
}

func (s *SelectElementAll) InTables(tables map[string]struct{}) error {
	if len(s.prefix) < 1 {
		return nil
	}
	if _, ok := tables[s.prefix]; ok {
		return nil
	}
	return errors.Errorf("unknown column '%s'", s.ToSelectString())
}

func (s *SelectElementAll) ToSelectString() string {
	var sb strings.Builder
	if len(s.prefix) > 0 {
		sb.WriteByte('`')
		sb.WriteString(s.prefix)
		sb.WriteByte('`')
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

func (s SelectElementAll) Mode() SelMode {
	return SelAll
}

type SelectElementExpr struct {
	inner ExpressionNode
	alias string
}

func (s *SelectElementExpr) InTables(tables map[string]struct{}) error {
	return s.inner.InTables(tables)
}

func (s *SelectElementExpr) ToSelectString() string {
	var sb strings.Builder
	_ = s.Expression().Restore(&sb, nil)
	return sb.String()
}

func (s *SelectElementExpr) Expression() ExpressionNode {
	return s.inner
}

func (s *SelectElementExpr) Alias() string {
	return s.alias
}

func (s *SelectElementExpr) Mode() SelMode {
	return SelExpr
}

type SelectElementFunction struct {
	inner interface{} // *Function or *AggrFunction
	alias string
}

func (s *SelectElementFunction) InTables(tables map[string]struct{}) error {
	switch val := s.inner.(type) {
	case inTablesChecker:
		return val.InTables(tables)
	default:
		return nil
	}
}

func (s *SelectElementFunction) ToSelectString() string {
	var sb strings.Builder
	switch fun := s.inner.(type) {
	case *Function:
		_ = fun.Restore(&sb, nil)
	case *AggrFunction:
		_ = fun.Restore(&sb, nil)
	case *CastFunction:
		_ = fun.Restore(&sb, nil)
	case *CaseWhenElseFunction:
		_ = fun.Restore(&sb, nil)
	default:
		panic("unreachable")
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

func (s *SelectElementFunction) Function() interface{} {
	return s.inner
}

func (s *SelectElementFunction) Alias() string {
	return s.alias
}

func (s *SelectElementFunction) Mode() SelMode {
	return SelFunc
}

type SelectElementColumn struct {
	name  []string
	alias string
}

func (s *SelectElementColumn) InTables(tables map[string]struct{}) error {
	if len(s.name) < 2 {
		return nil
	}
	if _, ok := tables[s.name[0]]; ok {
		return nil
	}
	return errors.Errorf("unknown column '%s'", s.ToSelectString())
}

func (s *SelectElementColumn) ToSelectString() string {
	var sb strings.Builder
	_ = ColumnNameExpressionAtom(s.name).Restore(&sb, nil)
	return sb.String()
}

func (s *SelectElementColumn) Name() []string {
	return s.name
}

func (s *SelectElementColumn) Alias() string {
	return s.alias
}

func (s *SelectElementColumn) Mode() SelMode {
	return SelCol
}

func NewSelectElementColumn(name []string, alias string) *SelectElementColumn {
	return &SelectElementColumn{
		name:  name,
		alias: alias,
	}
}

func NewSelectElementExpr(expr ExpressionNode, alias string) *SelectElementExpr {
	return &SelectElementExpr{
		inner: expr,
		alias: alias,
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
