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
	fmt.Stringer
	paramsCounter
	inTablesChecker
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

func (l *LikePredicateNode) String() string {
	var sb strings.Builder
	sb.WriteString(l.Left.String())
	if l.Not {
		sb.WriteString(" NOT")
	}
	sb.WriteString(" LIKE ")
	sb.WriteString(l.Right.String())
	return sb.String()
}

func (l *LikePredicateNode) CntParams() int {
	return l.Left.CntParams() + l.Right.CntParams()
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

func (b *BinaryComparisonPredicateNode) String() string {
	var (
		sb    strings.Builder
		op    = b.Op.String()
		left  = b.Left.String()
		right = b.Right.String()
	)

	// foo = NULL -> foo IS NULL
	// foo <> NULL / foo != NULL -> foo IS NOT NULL
	if strings.EqualFold(left, "NULL") || strings.EqualFold(right, "NULL") {
		switch op {
		case "=":
			op = "IS"
		case "<>", "!=":
			op = "IS NOT"
		}
	}

	sb.WriteString(left)
	sb.WriteByte(' ')
	sb.WriteString(op)
	sb.WriteByte(' ')
	sb.WriteString(right)
	return sb.String()
}

func (b *BinaryComparisonPredicateNode) CntParams() int {
	return b.Left.CntParams() + b.Right.CntParams()
}

func (b *BinaryComparisonPredicateNode) Mode() PredicateMode {
	return PmCompare
}

type AtomPredicateNode struct {
	A ExpressionAtom
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

func (a *AtomPredicateNode) String() string {
	return a.A.String()
}

func (a *AtomPredicateNode) CntParams() int {
	return a.A.CntParams()
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

func (b *BetweenPredicateNode) String() string {
	var sb strings.Builder
	sb.WriteString(b.Key.String())
	if b.Not {
		sb.WriteString(" NOT")
	}
	sb.WriteString(" BETWEEN ")
	sb.WriteString(b.Left.String())
	sb.WriteString(" AND ")
	sb.WriteString(b.Right.String())
	return sb.String()
}

func (b *BetweenPredicateNode) CntParams() int {
	return b.Key.CntParams() + b.Left.CntParams() + b.Right.CntParams()
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

func (ip *InPredicateNode) String() string {
	var sb strings.Builder

	sb.WriteString(ip.P.String())

	if ip.IsNot() {
		sb.WriteString(" NOT IN (")
	} else {
		sb.WriteString(" IN (")
	}

	sb.WriteString(ip.E[0].String())

	for i := 1; i < len(ip.E); i++ {
		sb.WriteByte(',')
		sb.WriteString(ip.E[i].String())
	}

	sb.WriteByte(')')
	return sb.String()
}

func (ip *InPredicateNode) CntParams() (n int) {
	n += ip.P.CntParams()
	for _, it := range ip.E {
		n += it.CntParams()
	}
	return
}

func (ip *InPredicateNode) Mode() PredicateMode {
	return PmIn
}
