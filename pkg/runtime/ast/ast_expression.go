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

import (
	"github.com/arana-db/arana/pkg/runtime/logical"
)

const (
	_ ExpressionMode = iota
	EmLogical
	EmPredicate
	EmNot
)

var (
	_ ExpressionNode = (*NotExpressionNode)(nil)
	_ ExpressionNode = (*PredicateExpressionNode)(nil)
	_ ExpressionNode = (*LogicalExpressionNode)(nil)
	_ ExpressionNode = (*NotExpressionNode)(nil)
)

type Restorer interface {
	Restore(sb *strings.Builder, args *[]int) error
}

type ExpressionMode uint8

type ExpressionNode interface {
	inTablesChecker
	Restorer
	Mode() ExpressionMode
}

type LogicalExpressionNode struct {
	Op    logical.Op
	Left  ExpressionNode
	Right ExpressionNode
}

func (l *LogicalExpressionNode) Restore(sb *strings.Builder, args *[]int) (err error) {
	if err = l.Left.Restore(sb, args); err != nil {
		return
	}

	sb.WriteByte(' ')
	sb.WriteString(l.Op.String())
	sb.WriteByte(' ')

	err = l.Right.Restore(sb, args)
	return
}

func (l *LogicalExpressionNode) InTables(tables map[string]struct{}) error {
	if err := l.Left.InTables(tables); err != nil {
		return err
	}
	if err := l.Right.InTables(tables); err != nil {
		return err
	}
	return nil
}

func (l *LogicalExpressionNode) Mode() ExpressionMode {
	return EmLogical
}

type NotExpressionNode struct {
	E ExpressionNode
}

func (n *NotExpressionNode) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString("NOT ")
	if err := n.E.Restore(sb, args); err != nil {
		return errors.Wrapf(err, "restore %T failed", n)
	}
	return nil
}

func (n *NotExpressionNode) InTables(tables map[string]struct{}) error {
	return n.E.InTables(tables)
}

func (n *NotExpressionNode) Mode() ExpressionMode {
	return EmNot
}

type PredicateExpressionNode struct {
	P PredicateNode
}

func (a *PredicateExpressionNode) InTables(tables map[string]struct{}) error {
	return a.P.InTables(tables)
}

func (a *PredicateExpressionNode) Restore(sb *strings.Builder, args *[]int) error {
	if err := a.P.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (a *PredicateExpressionNode) Mode() ExpressionMode {
	return EmPredicate
}
