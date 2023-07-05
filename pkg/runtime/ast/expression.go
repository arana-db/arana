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
)

type ExpressionMode uint8

type ExpressionNode interface {
	Node
	Restorer
	Mode() ExpressionMode
	Clone() ExpressionNode
}

type LogicalExpressionNode struct {
	Or    bool
	Left  ExpressionNode
	Right ExpressionNode
}

func (l *LogicalExpressionNode) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitLogicalExpression(l)
}

func (l *LogicalExpressionNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := l.Left.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if l.Or {
		sb.WriteString(" OR ")
	} else {
		sb.WriteString(" AND ")
	}

	if err := l.Right.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (l *LogicalExpressionNode) Mode() ExpressionMode {
	return EmLogical
}

func (l *LogicalExpressionNode) Clone() ExpressionNode {
	return &LogicalExpressionNode{
		Or:    l.Or,
		Left:  l.Left.Clone(),
		Right: l.Right.Clone(),
	}
}

type NotExpressionNode struct {
	E ExpressionNode
}

func (n *NotExpressionNode) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitNotExpression(n)
}

func (n *NotExpressionNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("NOT ")
	if err := n.E.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (n *NotExpressionNode) Mode() ExpressionMode {
	return EmNot
}

func (n *NotExpressionNode) Clone() ExpressionNode {
	return &NotExpressionNode{
		E: n.E.Clone(),
	}
}

type PredicateExpressionNode struct {
	P PredicateNode
}

func (a *PredicateExpressionNode) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitPredicateExpression(a)
}

func (a *PredicateExpressionNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := a.P.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (a *PredicateExpressionNode) Mode() ExpressionMode {
	return EmPredicate
}

func (a *PredicateExpressionNode) Clone() ExpressionNode {
	return &PredicateExpressionNode{
		P: a.P.Clone(),
	}
}
