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
	"github.com/dubbogo/arana/pkg/runtime/cmp"
)

const (
	_ PredicateMode = iota
	PmIn
	PmCompare
	PmLike
	PmAtom
	PmBetween
)

var (
	_ PredicateNode = (*LikePredicateNode)(nil)
	_ PredicateNode = (*BinaryComparisonPredicateNode)(nil)
	_ PredicateNode = (*AtomPredicateNode)(nil)
	_ PredicateNode = (*InPredicateNode)(nil)
	_ PredicateNode = (*BetweenPredicateNode)(nil)
)

type PredicateMode uint8

type PredicateNode interface {
	inTablesChecker
	Restorer
	Mode() PredicateMode
}

type LikePredicateNode struct {
	Not   bool
	Left  PredicateNode
	Right PredicateNode
}

func (l *LikePredicateNode) InTables(tables map[string]struct{}) error {
	if err := l.Left.InTables(tables); err != nil {
		return err
	}
	if err := l.Right.InTables(tables); err != nil {
		return err
	}
	return nil
}

func (l *LikePredicateNode) Restore(sb *strings.Builder, args *[]int) error {
	if err := l.Left.Restore(sb, args); err != nil {
		return errors.Wrapf(err, "failed to restore %T", l)
	}

	if l.Not {
		sb.WriteString(" NOT LIKE")
	} else {
		sb.WriteString(" LIKE ")
	}

	if err := l.Right.Restore(sb, args); err != nil {
		return errors.Wrapf(err, "failed to restore %T", l)
	}

	return nil
}

func (l *LikePredicateNode) Mode() PredicateMode {
	return PmLike
}

type BinaryComparisonPredicateNode struct {
	Left  PredicateNode
	Right PredicateNode
	Op    cmp.Comparison
}

func (b *BinaryComparisonPredicateNode) InTables(tables map[string]struct{}) error {
	if err := b.Left.InTables(tables); err != nil {
		return err
	}
	if err := b.Right.InTables(tables); err != nil {
		return err
	}
	return nil
}

func (b *BinaryComparisonPredicateNode) Restore(sb *strings.Builder, args *[]int) error {
	var (
		op = b.Op.String()
	)

	// TODO: convert 'xxx=null' to 'xxx is null'
	// foo = NULL -> foo IS NULL
	// foo <> NULL / foo != NULL -> foo IS NOT NULL
	//if strings.EqualFold(left, "NULL") || strings.EqualFold(right, "NULL") {
	//	switch op {
	//	case "=":
	//		op = "IS"
	//	case "<>", "!=":
	//		op = "IS NOT"
	//	}
	//}
	if err := b.Left.Restore(sb, args); err != nil {
		return errors.Wrapf(err, "faile to restore %T", b)
	}

	sb.WriteByte(' ')
	sb.WriteString(op)
	sb.WriteByte(' ')

	if err := b.Right.Restore(sb, args); err != nil {
		return errors.Wrapf(err, "faile to restore %T", b)
	}

	return nil
}

func (b *BinaryComparisonPredicateNode) Mode() PredicateMode {
	return PmCompare
}

type AtomPredicateNode struct {
	A ExpressionAtom
}

func (a *AtomPredicateNode) Restore(sb *strings.Builder, args *[]int) error {
	if err := a.A.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (a *AtomPredicateNode) InTables(tables map[string]struct{}) error {
	return a.A.InTables(tables)
}

func (a *AtomPredicateNode) Column() (ColumnNameExpressionAtom, bool) {
	switch v := a.A.(type) {
	case ColumnNameExpressionAtom:
		return v, true
	}
	return nil, false
}

func (a *AtomPredicateNode) Mode() PredicateMode {
	return PmAtom
}

type BetweenPredicateNode struct {
	Not   bool
	Key   PredicateNode
	Left  PredicateNode
	Right PredicateNode
}

func (b *BetweenPredicateNode) InTables(tables map[string]struct{}) error {
	if err := b.Key.InTables(tables); err != nil {
		return err
	}
	if err := b.Left.InTables(tables); err != nil {
		return err
	}
	if err := b.Right.InTables(tables); err != nil {
		return err
	}
	return nil
}

func (b *BetweenPredicateNode) Restore(sb *strings.Builder, args *[]int) error {
	if err := b.Key.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	if b.Not {
		sb.WriteString(" NOT BETWEEN")
	} else {
		sb.WriteString(" BETWEEN ")
	}

	if err := b.Left.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}
	sb.WriteString(" AND ")
	if err := b.Right.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (b *BetweenPredicateNode) Mode() PredicateMode {
	return PmBetween
}

type InPredicateNode struct {
	not bool
	P   PredicateNode
	E   []ExpressionNode
	// TODO: select statement
}

func (ip *InPredicateNode) InTables(tables map[string]struct{}) error {
	if err := ip.P.InTables(tables); err != nil {
		return err
	}
	for _, it := range ip.E {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (ip *InPredicateNode) IsNot() bool {
	return ip.not
}

func (ip *InPredicateNode) Restore(sb *strings.Builder, args *[]int) error {
	if err := ip.P.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	if ip.IsNot() {
		sb.WriteString(" NOT IN (")
	} else {
		sb.WriteString(" IN (")
	}

	if err := ip.E[0].Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}
	for i := 1; i < len(ip.E); i++ {
		sb.WriteString(", ")
		if err := ip.E[i].Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	sb.WriteByte(')')

	return nil
}

func (ip *InPredicateNode) Mode() PredicateMode {
	return PmIn
}
