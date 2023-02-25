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

type TableName []string

func (t TableName) ResetSuffix(suffix string) TableName {
	if t == nil {
		return nil
	}

	if len(t) == 0 {
		return TableName{}
	}

	ret := make(TableName, len(t))
	copy(ret, t)
	ret[len(ret)-1] = suffix

	return ret
}

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

// TableToTable represents renaming old table to new table used in RenameTableStmt.
type TableToTable struct {
	OldTable *TableName
	NewTable *TableName
}

func (t TableToTable) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	return errors.Errorf("unimplement: tableToTable restore!")
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
	Expr ExpressionAtom
	Desc bool
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

func (ln *LimitNode) UnsetOffsetVar() {
	ln.flag &= ^flagLimitOffsetVar
}

func (ln *LimitNode) UnsetLimitVar() {
	ln.flag &= ^flagLimitLimitVar
}

func (ln *LimitNode) SetHasOffset() {
	ln.flag |= flagLimitHasOffset
}

func (ln *LimitNode) UnsetHasOffset() {
	ln.flag &= ^flagLimitHasOffset
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

type ColumnOptionType uint8

const (
	_ ColumnOptionType = iota
	ColumnOptionPrimaryKey
	ColumnOptionNotNull
	ColumnOptionAutoIncrement
	ColumnOptionDefaultValue
	ColumnOptionUniqKey
	ColumnOptionNull
	ColumnOptionComment
	ColumnOptionCollate
	ColumnOptionColumnFormat
	ColumnOptionStorage
)

type ColumnOption struct {
	Tp     ColumnOptionType
	Expr   ExpressionNode
	StrVal string
}

func (c *ColumnOption) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch c.Tp {
	case ColumnOptionPrimaryKey:
		sb.WriteString("PRIMARY KEY")
	case ColumnOptionNotNull:
		sb.WriteString("NOT NULL")
	case ColumnOptionAutoIncrement:
		sb.WriteString("AUTO_INCREMENT")
	case ColumnOptionDefaultValue:
		sb.WriteString("DEFAULT ")
		if err := c.Expr.Restore(flag, sb, args); err != nil {
			return err
		}
	case ColumnOptionUniqKey:
		sb.WriteString("UNIQUE KEY")
	case ColumnOptionNull:
		sb.WriteString("NULL")
	case ColumnOptionComment:
		sb.WriteString("COMMENT ")
		if err := c.Expr.Restore(flag, sb, args); err != nil {
			return err
		}
	case ColumnOptionCollate:
		if len(c.StrVal) == 0 {
			return errors.New("Empty ColumnOption COLLATE")
		}
		sb.WriteString("COLLATE ")
		sb.WriteString(c.StrVal)
	case ColumnOptionColumnFormat:
		sb.WriteString("COLUMN_FORMAT ")
		sb.WriteString(c.StrVal)
	case ColumnOptionStorage:
		sb.WriteString("STORAGE ")
		sb.WriteString(c.StrVal)
	}
	return nil
}

func (c *ColumnOption) CntParams() int {
	return 0
}

type ColumnDefine struct {
	Column  ColumnNameExpressionAtom
	Tp      string
	Options []*ColumnOption
}

func (c *ColumnDefine) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := c.Column.Restore(flag, sb, args); err != nil {
		return err
	}
	if len(c.Tp) > 0 {
		sb.WriteString(" " + c.Tp)
	}
	for _, o := range c.Options {
		sb.WriteString(" ")
		if err := o.Restore(flag, sb, args); err != nil {
			return err
		}
	}
	return nil
}

func (c *ColumnDefine) CntParams() int {
	return 0
}

type ColumnPositionType uint8

const (
	_ ColumnPositionType = iota
	ColumnPositionFirst
	ColumnPositionAfter
)

type ColumnPosition struct {
	Tp     ColumnPositionType
	Column ColumnNameExpressionAtom
}

func (c *ColumnPosition) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch c.Tp {
	case ColumnPositionFirst:
		sb.WriteString("FIRST")
	case ColumnPositionAfter:
		sb.WriteString("AFTER ")
		if err := c.Column.Restore(flag, sb, args); err != nil {
			return err
		}
	default:
		return errors.New("invalid ColumnPositionType")
	}
	return nil
}

func (c *ColumnPosition) CntParams() int {
	return 0
}

type IndexPartSpec struct {
	Column ColumnNameExpressionAtom
	Expr   ExpressionNode
}

func (i *IndexPartSpec) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if i.Expr != nil {
		sb.WriteString("(")
		if err := i.Expr.Restore(flag, sb, args); err != nil {
			return err
		}
		sb.WriteString(")")
		return nil
	}
	if err := i.Column.Restore(flag, sb, args); err != nil {
		return err
	}
	return nil
}

func (i *IndexPartSpec) CntParams() int {
	return 0
}

// IndexOption is the index options.
//
//	  KEY_BLOCK_SIZE [=] value
//	| index_type
//	| WITH PARSER parser_name
//	| COMMENT 'string'
//
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexOption struct {
	KeyBlockSize uint64
	Tp           IndexType
	Comment      string
	ParserName   string
}

func (i *IndexOption) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	hasPrevOption := false

	if i.KeyBlockSize > 0 {
		if _, err := fmt.Fprintf(sb, "KEY_BLOCK_SIZE=%d", i.KeyBlockSize); err != nil {
			return err
		}
		hasPrevOption = true
	}

	if i.Tp != IndexTypeInvalid {
		if hasPrevOption {
			sb.WriteString(" ")
		}
		sb.WriteString("USING ")
		sb.WriteString(i.Tp.String())
		hasPrevOption = true
	}

	// parser name
	if len(i.ParserName) != 0 {
		if hasPrevOption {
			sb.WriteString(" ")
		}
		sb.WriteString("WITH PARSER ")
		sb.WriteString(i.ParserName)
		hasPrevOption = true
	}

	// comment
	if len(i.Comment) != 0 {
		if hasPrevOption {
			sb.WriteString(" ")
		}
		sb.WriteString("COMMENT ")
		sb.WriteString(i.Comment)
	}
	return nil
}

type IndexLockAndAlgorithm struct {
	LockTp      LockType
	AlgorithmTp AlgorithmType
}

func (i *IndexLockAndAlgorithm) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	hasPrevOption := false
	if i.AlgorithmTp != AlgorithmTypeDefault {
		sb.WriteString("ALGORITHM")
		sb.WriteString(" = ")
		sb.WriteString(i.AlgorithmTp.String())
		hasPrevOption = true
	}

	if i.LockTp != LockTypeDefault {
		if hasPrevOption {
			sb.WriteString(" ")
		}
		sb.WriteString("LOCK")
		sb.WriteString(" = ")
		sb.WriteString(i.LockTp.String())
	}
	return nil
}

type ConstraintType uint8

const (
	ConstraintNoConstraint ConstraintType = iota
	ConstraintPrimaryKey
	ConstraintKey
	ConstraintIndex
	ConstraintUniq
	ConstraintUniqKey
	ConstraintUniqIndex
	ConstraintFulltext
)

type Constraint struct {
	Tp   ConstraintType
	Name string
	Keys []*IndexPartSpec
}

func (c *Constraint) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch c.Tp {
	case ConstraintPrimaryKey:
		sb.WriteString("PRIMARY KEY")
	case ConstraintKey:
		sb.WriteString("KEY")
	case ConstraintIndex:
		sb.WriteString("INDEX")
	case ConstraintUniq:
		sb.WriteString("UNIQUE")
	case ConstraintUniqKey:
		sb.WriteString("UNIQUE KEY")
	case ConstraintUniqIndex:
		sb.WriteString("UNIQUE INDEX")
	case ConstraintFulltext:
		sb.WriteString("FULLTEXT")
	}
	if len(c.Name) > 0 {
		sb.WriteString(" ")
		sb.WriteString(c.Name)
	}
	sb.WriteString("(")
	for i, k := range c.Keys {
		if i != 0 {
			sb.WriteString(", ")
		}
		if err := k.Restore(flag, sb, args); err != nil {
			return err
		}
	}
	sb.WriteString(")")
	return nil
}

func (c *Constraint) CntParams() int {
	return 0
}
