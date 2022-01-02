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

package xxast

import (
	"fmt"
	"strings"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/logical"
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

type ExpressionMode uint8

type ExpressionNode interface {
	fmt.Stringer
	paramsCounter
	inTablesChecker
	Mode() ExpressionMode
}

type LogicalExpressionNode struct {
	Op    logical.Op
	Left  ExpressionNode
	Right ExpressionNode
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

func (l *LogicalExpressionNode) String() string {
	var sb strings.Builder

	sb.WriteString(l.Left.String())
	switch l.Op {
	case logical.Land:
		sb.WriteString(" AND ")
	case logical.Lor:
		sb.WriteString(" OR ")
	default:
		panic("unreachable")
	}

	sb.WriteString(l.Right.String())
	return sb.String()
}

func (l *LogicalExpressionNode) CntParams() int {
	return l.Left.CntParams() + l.Right.CntParams()
}

func (l *LogicalExpressionNode) Mode() ExpressionMode {
	return EmLogical
}

type NotExpressionNode struct {
	E ExpressionNode
}

func (n *NotExpressionNode) InTables(tables map[string]struct{}) error {
	return n.E.InTables(tables)
}

func (n *NotExpressionNode) String() string {
	var sb strings.Builder
	sb.WriteString("NOT ")
	sb.WriteString(n.E.String())
	return sb.String()
}

func (n *NotExpressionNode) CntParams() int {
	return n.E.CntParams()
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

func (a *PredicateExpressionNode) String() string {
	return a.P.String()
}

func (a *PredicateExpressionNode) CntParams() int {
	return a.P.CntParams()
}

func (a *PredicateExpressionNode) Mode() ExpressionMode {
	return EmPredicate
}
