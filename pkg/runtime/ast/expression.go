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

type ExpressionMode uint8

type ExpressionNode interface {
	Restorer
	paramsCounter
	Mode() ExpressionMode
}

type LogicalExpressionNode struct {
	Op    logical.Op
	Left  ExpressionNode
	Right ExpressionNode
}

func (l *LogicalExpressionNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := l.Left.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	switch l.Op {
	case logical.Land:
		sb.WriteString(" AND ")
	case logical.Lor:
		sb.WriteString(" OR ")
	default:
		panic("unreachable")
	}

	if err := l.Right.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
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

func (n *NotExpressionNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("NOT ")
	if err := n.E.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
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

func (a *PredicateExpressionNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := a.P.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (a *PredicateExpressionNode) CntParams() int {
	return a.P.CntParams()
}

func (a *PredicateExpressionNode) Mode() ExpressionMode {
	return EmPredicate
}
