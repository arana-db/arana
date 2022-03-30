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
	"fmt"
	"strings"
)

import (
	"github.com/pkg/errors"
)

var (
	_ inTablesChecker = (OrderByNode)(nil)
	_ inTablesChecker = (*OrderByItem)(nil)
	_ inTablesChecker = (*GroupByNode)(nil)
	_ inTablesChecker = (*GroupByItem)(nil)
)

type TableName []string

func (t TableName) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	WriteID(sb, t[0])

	for i := 1; i < len(t); i++ {
		sb.WriteByte('.')
		WriteID(sb, t[i])
	}
	return nil
}

func (t TableName) String() string {
	return MustRestoreToString(RestoreDefault, t)
}

func (t TableName) Prefix() (prefix string) {
	if len(t) > 1 {
		prefix = t[0]
	}
	return
}

func (t TableName) Suffix() string {
	return t[len(t)-1]
}

const (
	_ indexHintType = iota
	indexHintTypeJoin
	indexHintTypeOrderBy
	indexHintTypeGroupBy
)

var _indexHintTypeNames = [...]string{
	indexHintTypeJoin:    "JOIN",
	indexHintTypeOrderBy: "ORDER BY",
	indexHintTypeGroupBy: "GROUP BY",
}

type indexHintType uint8

func (i indexHintType) String() string {
	return _indexHintTypeNames[i]
}

const (
	indexHintActionUse indexHintAction = iota
	indexHintActionIgnore
	indexHintActionForce
)

var _indexHintActionNames = [...]string{
	indexHintActionUse:    "USE",
	indexHintActionIgnore: "IGNORE",
	indexHintActionForce:  "FORCE",
}

type indexHintAction uint8

func (i indexHintAction) String() string {
	return _indexHintActionNames[i]
}

type IndexHint struct {
	action        indexHintAction
	useKey        bool
	indexHintType indexHintType
	indexes       []string
}

func (ih *IndexHint) Action() string {
	return ih.action.String()
}

func (ih *IndexHint) KeyFormat() string {
	if ih.useKey {
		return "KEY"
	}
	return "INDEX"
}

func (ih *IndexHint) Indexes() []string {
	return ih.indexes
}

func (ih *IndexHint) IndexHintType() (string, bool) {
	if ih.indexHintType == 0 {
		return "", false
	}
	return ih.indexHintType.String(), true
}

func (ih *IndexHint) Restore(flag RestoreFlag, sb *strings.Builder, _ *[]int) error {
	sb.WriteString(ih.Action())
	sb.WriteByte(' ')
	sb.WriteString(ih.KeyFormat())
	if t, ok := ih.IndexHintType(); ok {
		sb.WriteString(" FOR ")
		sb.WriteString(t)
	}

	sb.WriteByte('(')

	WriteID(sb, ih.indexes[0])

	for i := 1; i < len(ih.indexes); i++ {
		sb.WriteByte(',')

		WriteID(sb, ih.indexes[i])
	}

	sb.WriteByte(')')

	return nil
}

type OrderByItem struct {
	Alias string
	Expr  ExpressionAtom
	Desc  bool
}

func (o OrderByItem) InTables(tables map[string]struct{}) error {
	if o.Expr != nil {
		if err := o.Expr.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (o OrderByItem) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := o.Expr.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	if o.Desc {
		sb.WriteString(" DESC")
	}
	return nil
}

const (
	flagGroupByHasOrder uint8 = 0x01 << iota
	flagGroupByOrderDesc
)

type GroupByItem struct {
	expr ExpressionNode
	flag uint8
}

func (gb *GroupByItem) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := gb.expr.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	if !gb.HasOrder() {
		return nil
	}

	if gb.IsOrderDesc() {
		sb.WriteString(" DESC")
	} else {
		sb.WriteString(" ASC")
	}

	return nil
}

func (gb *GroupByItem) InTables(tables map[string]struct{}) error {
	return gb.expr.InTables(tables)
}

func (gb *GroupByItem) Expr() ExpressionNode {
	return gb.expr
}

func (gb *GroupByItem) HasOrder() bool {
	return gb.flag&flagGroupByHasOrder != 0
}

func (gb *GroupByItem) IsOrderDesc() bool {
	return gb.flag&flagGroupByOrderDesc != 0
}

type OrderByNode []*OrderByItem

func (o OrderByNode) String() string {
	return MustRestoreToString(RestoreDefault, o)
}

func (o OrderByNode) InTables(tables map[string]struct{}) error {
	for _, it := range o {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (o OrderByNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if len(o) < 1 {
		return nil
	}

	if err := o[0].Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	for i := 1; i < len(o); i++ {
		sb.WriteString(", ")
		if err := o[i].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

const (
	flagLimitHasOffset uint8 = 1 << iota
	flagLimitOffsetVar
	flagLimitLimitVar
)

type LimitNode struct {
	flag   uint8
	offset int64
	limit  int64
}

func (ln *LimitNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if ln.HasOffset() {
		if ln.IsOffsetVar() {
			sb.WriteByte('?')

			if args != nil {
				*args = append(*args, int(ln.offset))
			}
		} else {
			_, _ = fmt.Fprintf(sb, "%d", ln.offset)
		}
		sb.WriteByte(',')
	}
	if ln.IsLimitVar() {
		sb.WriteByte('?')
		if args != nil {
			*args = append(*args, int(ln.limit))
		}
	} else {
		_, _ = fmt.Fprintf(sb, "%d", ln.limit)
	}
	return nil
}

func (ln *LimitNode) SetOffsetVar() {
	ln.flag |= flagLimitOffsetVar
}

func (ln *LimitNode) SetLimitVar() {
	ln.flag |= flagLimitLimitVar
}

func (ln *LimitNode) SetHasOffset() {
	ln.flag |= flagLimitHasOffset
}

func (ln *LimitNode) HasOffset() bool {
	return ln.flag&flagLimitHasOffset != 0
}

func (ln *LimitNode) IsOffsetVar() bool {
	return ln.flag&flagLimitOffsetVar != 0
}

func (ln *LimitNode) IsLimitVar() bool {
	return ln.flag&flagLimitLimitVar != 0
}

func (ln *LimitNode) Offset() int64 {
	return ln.offset
}

func (ln *LimitNode) Limit() int64 {
	return ln.limit
}

func (ln *LimitNode) SetLimit(n int64) {
	ln.limit = n
}

func (ln *LimitNode) SetOffset(n int64) {
	ln.offset = n
}

type FromNode = []*TableSourceNode

type SelectNode = []SelectElement

type GroupByNode struct {
	RollUp bool
	Items  []*GroupByItem
}

func (g *GroupByNode) InTables(tables map[string]struct{}) error {
	for _, it := range g.Items {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

type UpdateElement struct {
	Column ColumnNameExpressionAtom
	Value  ExpressionNode
}

func (u *UpdateElement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := u.Column.Restore(flag, sb, args); err != nil {
		return err
	}
	sb.WriteString(" = ")
	if err := u.Value.Restore(flag, sb, args); err != nil {
		return err
	}
	return nil
}

func (u *UpdateElement) CntParams() int {
	if u.Value == nil {
		return 0
	}
	return u.Value.CntParams()
}
