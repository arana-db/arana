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
	Restorer
	inTablesChecker
	// Alias returns the alias if available.
	Alias() string
	// Mode returns the SelMode.
	Mode() SelMode
	// ToSelectString converts current SelectElement to SQL SELECT string and return it.
	ToSelectString() string
	// Hacked returns true if current SelectElement is hacked by optimizer.
	Hacked() bool
	// SetHacked sets hacked in bool.
	SetHacked(hacked bool)
}

type SelectElementAll struct {
	prefix string
	hacked bool
}

func (s *SelectElementAll) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if prefix := s.Prefix(); len(prefix) > 0 {
		sb.WriteString(prefix)
		sb.WriteByte('.')
	}
	sb.WriteByte('*')
	return nil
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

func (s *SelectElementAll) Hacked() bool {
	return s.hacked
}

func (s *SelectElementAll) SetHacked(hacked bool) {
	s.hacked = hacked
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

func (s SelectElementAll) Mode() SelMode {
	return SelAll
}

type SelectElementExpr struct {
	inner  ExpressionNode
	alias  string
	hacked bool
}

func (s *SelectElementExpr) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := s.Expression().Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if len(s.alias) > 0 {
		sb.WriteString(" AS ")
		WriteID(sb, s.alias)
	}

	return nil
}

func (s *SelectElementExpr) InTables(tables map[string]struct{}) error {
	return s.inner.InTables(tables)
}

func (s *SelectElementExpr) Hacked() bool {
	return s.hacked
}

func (s *SelectElementExpr) SetHacked(hacked bool) {
	s.hacked = hacked
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

func (s *SelectElementExpr) Mode() SelMode {
	return SelExpr
}

type SelectElementFunction struct {
	inner  interface{} // *Function or *AggrFunction
	alias  string
	hacked bool
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

	if len(s.alias) > 0 {
		sb.WriteString(" AS ")
		WriteID(sb, s.alias)
	}

	return nil
}

func (s *SelectElementFunction) InTables(tables map[string]struct{}) error {
	switch val := s.inner.(type) {
	case inTablesChecker:
		return val.InTables(tables)
	default:
		return nil
	}
}

func (s *SelectElementFunction) Hacked() bool {
	return s.hacked
}

func (s *SelectElementFunction) SetHacked(hacked bool) {
	s.hacked = hacked
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
	name   []string
	alias  string
	hacked bool
}

func (s *SelectElementColumn) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := ColumnNameExpressionAtom(s.Name()).Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	if len(s.alias) > 0 {
		sb.WriteString(" AS ")
		WriteID(sb, s.alias)
	}
	return nil
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

func (s *SelectElementColumn) Hacked() bool {
	return s.hacked
}

func (s *SelectElementColumn) SetHacked(hacked bool) {
	s.hacked = hacked
}

func (s *SelectElementColumn) ToSelectString() string {
	return ColumnNameExpressionAtom(s.name).String()
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
