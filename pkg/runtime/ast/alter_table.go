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

var _ Statement = (*AlterTableStatement)(nil)

type AlterTableType uint8

const (
	_ AlterTableType = iota
	AlterTableAddColumns
	AlterTableDropColumn
	AlterTableAddConstraint
	AlterTableChangeColumn
	AlterTableModifyColumn
	AlterTableRenameTable
	AlterTableRenameColumn
)

type AlterTableSpecStatement struct {
	Tp            AlterTableType
	OldColumnName ColumnNameExpressionAtom
	NewColumnName ColumnNameExpressionAtom
	NewColumns    []*ColumnDefine
	NewTable      TableName
	Position      *ColumnPosition
	Constraint    *Constraint
}

func (a *AlterTableSpecStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch a.Tp {
	case AlterTableDropColumn:
		sb.WriteString("DROP COLUMN ")
		if err := a.OldColumnName.Restore(flag, sb, args); err != nil {
			return err
		}
	case AlterTableAddColumns:
		sb.WriteString("ADD COLUMN ")
		if len(a.NewColumns) == 1 {
			if err := a.NewColumns[0].Restore(flag, sb, args); err != nil {
				return err
			}
			if a.Position != nil {
				sb.WriteString(" ")
				if err := a.Position.Restore(flag, sb, args); err != nil {
					return err
				}
			}
		} else {
			sb.WriteString("(")
			for i, col := range a.NewColumns {
				if i != 0 {
					sb.WriteString(", ")
				}
				if err := col.Restore(flag, sb, args); err != nil {
					return err
				}
			}
			sb.WriteString(")")
		}
	case AlterTableAddConstraint:
		sb.WriteString("ADD ")
		if err := a.Constraint.Restore(flag, sb, args); err != nil {
			return err
		}
	case AlterTableChangeColumn:
		sb.WriteString("CHANGE COLUMN ")
		if err := a.OldColumnName.Restore(flag, sb, args); err != nil {
			return err
		}
		sb.WriteString(" ")
		if err := a.NewColumns[0].Restore(flag, sb, args); err != nil {
			return err
		}
		if a.Position != nil {
			sb.WriteString(" ")
			if err := a.Position.Restore(flag, sb, args); err != nil {
				return err
			}
		}
	case AlterTableModifyColumn:
		sb.WriteString("MODIFY COLUMN ")
		if err := a.NewColumns[0].Restore(flag, sb, args); err != nil {
			return err
		}
		if a.Position != nil {
			sb.WriteString(" ")
			if err := a.Position.Restore(flag, sb, args); err != nil {
				return err
			}
		}
	case AlterTableRenameTable:
		sb.WriteString("RENAME AS ")
		if err := a.NewTable.Restore(flag, sb, args); err != nil {
			return err
		}
	case AlterTableRenameColumn:
		sb.WriteString("RENAME COLUMN ")
		if err := a.OldColumnName.Restore(flag, sb, args); err != nil {
			return err
		}
		sb.WriteString(" TO ")
		if err := a.NewColumnName.Restore(flag, sb, args); err != nil {
			return err
		}
	}

	return nil
}

// AlterTableStatement represents mysql alter table statement. see https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
type AlterTableStatement struct {
	Table TableName
	Specs []*AlterTableSpecStatement
}

func (at *AlterTableStatement) ResetTable(table string) *AlterTableStatement {
	ret := new(AlterTableStatement)
	*ret = *at

	tableName := make(TableName, len(ret.Table))
	copy(tableName, ret.Table)
	tableName[len(tableName)-1] = table

	ret.Table = tableName
	return ret
}

func (at *AlterTableStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("ALTER TABLE ")
	if err := at.Table.Restore(flag, sb, args); err != nil {
		return err
	}
	for i, spec := range at.Specs {
		if i == 0 {
			sb.WriteString(" ")
		} else {
			sb.WriteString(", ")
		}
		if err := spec.Restore(flag, sb, args); err != nil {
			return err
		}
	}
	return nil
}

func (at *AlterTableStatement) Mode() SQLType {
	return SQLTypeAlterTable
}
