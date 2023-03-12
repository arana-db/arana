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
	_flagInsertIgnore uint8 = 1 << iota
	_flagInsertLowPriority
	_flagInsertDelayed
	_flagInsertHighPriority
	_flagInsertSetSyntax
)

type BaseInsertStatement interface {
	Statement
	IsSetSyntax() bool
	IsIgnore() bool
	Priority() (string, bool)
}

type baseInsertStatement struct {
	flag    uint8
	Table   TableName
	Columns []string
}

func (b *baseInsertStatement) IsSetSyntax() bool {
	return b.flag&_flagInsertSetSyntax != 0
}

func (b *baseInsertStatement) IsLowPriority() bool {
	return b.flag&_flagInsertLowPriority != 0
}

func (b *baseInsertStatement) IsHighPriority() bool {
	return b.flag&_flagInsertHighPriority != 0
}

func (b *baseInsertStatement) IsDelayed() bool {
	return b.flag&_flagInsertDelayed != 0
}

func (b *baseInsertStatement) IsIgnore() bool {
	return b.flag&_flagInsertIgnore != 0
}

func (b *baseInsertStatement) Priority() (priority string, ok bool) {
	if b.flag&_flagInsertHighPriority != 0 {
		priority = "HIGH_PRIORITY"
		ok = true
	} else if b.flag&_flagInsertLowPriority != 0 {
		priority = "LOW_PRIORITY"
		ok = true
	} else if b.flag&_flagInsertDelayed != 0 {
		priority = "DELAYED"
		ok = true
	}
	return
}

func (b *baseInsertStatement) SetFlag(flag uint8) {
	b.flag = flag
}

func (b *baseInsertStatement) Flag() uint8 {
	return b.flag
}

func (b *baseInsertStatement) enableIgnore() {
	b.flag |= _flagInsertIgnore
}

func (b *baseInsertStatement) enableLowPriority() {
	b.flag |= _flagInsertLowPriority
}

func (b *baseInsertStatement) enableHighPriority() {
	b.flag |= _flagInsertHighPriority
}

func (b *baseInsertStatement) enableDelayedPriority() {
	b.flag |= _flagInsertDelayed
}

func (b *baseInsertStatement) enableSetSyntax() {
	b.flag |= _flagInsertSetSyntax
}

type ReplaceStatement struct {
	*baseInsertStatement
	Values [][]ExpressionNode
}

func (r *ReplaceStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	// TODO implement me
	panic("implement me")
}

func (r *ReplaceStatement) Mode() SQLType {
	return SQLTypeReplace
}

// InsertStatement represents mysql insert statement. see https://dev.mysql.com/doc/refman/8.0/en/insert.html
type InsertStatement struct {
	*baseInsertStatement
	DuplicatedUpdates []*UpdateElement
	Values            [][]ExpressionNode
}

func NewInsertStatement(table TableName, columns []string) *InsertStatement {
	return &InsertStatement{
		baseInsertStatement: &baseInsertStatement{
			Table:   table,
			Columns: columns,
		},
	}
}

func (is *InsertStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("INSERT ")

	// write priority
	if is.IsLowPriority() {
		sb.WriteString("LOW_PRIORITY ")
	} else if is.IsHighPriority() {
		sb.WriteString("HIGH_PRIORITY ")
	} else if is.IsDelayed() {
		sb.WriteString("DELAYED ")
	}

	if is.IsIgnore() {
		sb.WriteString("IGNORE ")
	}

	sb.WriteString("INTO ")

	if err := is.Table.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if is.IsSetSyntax() {
		sb.WriteString(" SET ")
		_ = is.Columns[0]
		_ = is.Values[0]

		if len(is.Columns) != len(is.Values[0]) {
			return errors.Errorf("length of column and value doesn't match: %d<>%d", len(is.Columns), len(is.Values[0]))
		}

		WriteID(sb, is.Columns[0])
		sb.WriteString(" = ")
		if err := is.Values[0][0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}

		for i := 1; i < len(is.Columns); i++ {
			sb.WriteString(", ")
			WriteID(sb, is.Columns[i])
			sb.WriteString(" = ")
			if err := is.Values[0][i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	} else if len(is.Columns) > 0 {
		sb.WriteByte('(')
		WriteID(sb, is.Columns[0])
		for i := 1; i < len(is.Columns); i++ {
			sb.WriteString(", ")
			WriteID(sb, is.Columns[i])
		}
		sb.WriteString(") ")
	} else {
		sb.WriteByte(' ')
	}

	if !is.IsSetSyntax() {
		sb.WriteString("VALUES ")

		writeOne := func(flag RestoreFlag, sb *strings.Builder, args *[]int, values []ExpressionNode) error {
			sb.WriteByte('(')

			if len(values) > 0 {
				if err := values[0].Restore(flag, sb, args); err != nil {
					return errors.WithStack(err)
				}
				for i := 1; i < len(values); i++ {
					sb.WriteString(", ")
					if err := values[i].Restore(flag, sb, args); err != nil {
						return errors.WithStack(err)
					}
				}

			}

			sb.WriteByte(')')

			return nil
		}

		if err := writeOne(flag, sb, args, is.Values[0]); err != nil {
			return errors.WithStack(err)
		}

		for i := 1; i < len(is.Values); i++ {
			sb.WriteByte(',')
			if err := writeOne(flag, sb, args, is.Values[i]); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	if len(is.DuplicatedUpdates) > 0 {
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")

		if err := is.DuplicatedUpdates[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		for i := 1; i < len(is.DuplicatedUpdates); i++ {
			sb.WriteString(", ")
			if err := is.DuplicatedUpdates[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func (is *InsertStatement) Mode() SQLType {
	return SQLTypeInsert
}

type ReplaceSelectStatement struct {
	*baseInsertStatement
	Select *SelectStatement
}

func (r *ReplaceSelectStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	// TODO implement me
	panic("implement me")
}

func (r *ReplaceSelectStatement) Mode() SQLType {
	return SQLTypeReplace
}

type InsertSelectStatement struct {
	*baseInsertStatement
	duplicatedUpdates []*UpdateElement
	sel               *SelectStatement
	unionSel          *UnionSelectStatement
}

func (is *InsertSelectStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("INSERT ")

	// write priority
	if is.IsLowPriority() {
		sb.WriteString("LOW_PRIORITY ")
	} else if is.IsHighPriority() {
		sb.WriteString("HIGH_PRIORITY ")
	} else if is.IsDelayed() {
		sb.WriteString("DELAYED ")
	}

	if is.IsIgnore() {
		sb.WriteString("IGNORE ")
	}

	sb.WriteString("INTO ")

	if err := is.Table.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if len(is.Columns) > 0 {
		sb.WriteByte('(')
		WriteID(sb, is.Columns[0])
		for i := 1; i < len(is.Columns); i++ {
			sb.WriteString(", ")
			WriteID(sb, is.Columns[i])
		}
		sb.WriteString(") ")
	} else {
		sb.WriteByte(' ')
	}

	if is.sel != nil {
		if err := is.sel.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if is.unionSel != nil {
		if err := is.unionSel.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if len(is.duplicatedUpdates) > 0 {
		sb.WriteString(" ON DUPLICATE KEY UPDATE ")

		if err := is.duplicatedUpdates[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		for i := 1; i < len(is.duplicatedUpdates); i++ {
			sb.WriteString(", ")
			if err := is.duplicatedUpdates[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func (is *InsertSelectStatement) Select() *SelectStatement {
	return is.sel
}

func (is *InsertSelectStatement) Mode() SQLType {
	return SQLTypeInsertSelect
}
