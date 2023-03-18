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
	"github.com/arana-db/parser/ast"
	"github.com/arana-db/parser/format"

	"github.com/pkg/errors"
)

var _ Statement = (*CreateTableStmt)(nil)

type CreateTableStmt struct {
	//IfNotExists bool
	//TemporaryKeyword
	// Meanless when TemporaryKeyword is not TemporaryGlobal.
	// ON COMMIT DELETE ROWS => true
	// ON COMMIT PRESERVE ROW => false
	//OnCommitDelete bool
	Table       *TableName
	ReferTable  *TableName
	Cols        []*ast.ColumnDef
	Constraints []*ast.Constraint
	Options     []*ast.TableOption
	//Partition      *PartitionOptions
	//OnDuplicate    OnDuplicateKeyHandlingType
	//Select         ResultSetNode
}

func NewCreateTableStmt() *CreateTableStmt {
	return &CreateTableStmt{}
}

func (c *CreateTableStmt) CntParams() int {
	return 1
}

func (c *CreateTableStmt) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("CREATE TABLE ")
	if err := c.Table.Restore(flag, sb, args); err != nil {
		return errors.Wrapf(err, "an error occurred while restore AnalyzeTableStatement.Tables[%s]", c.Table)
	}

	if c.ReferTable != nil {
		sb.WriteString(" LIKE ")
		if err := c.ReferTable.Restore(flag, sb, args); err != nil {
			return errors.Wrapf(err, "An error occurred while splicing CreateTableStmt ReferTable")
		}
	}

	rsCtx := format.NewRestoreCtx(format.RestoreFlags(flag), sb)

	lenCols := len(c.Cols)
	lenConstraints := len(c.Constraints)
	if lenCols+lenConstraints > 0 {
		sb.WriteString(" (")
		for i, col := range c.Cols {
			if i > 0 {
				sb.WriteString(",")
			}
			if err := col.Restore(rsCtx); err != nil {
				return errors.Wrapf(err, "An error occurred while splicing CreateTableStmt ColumnDef: [%v]", i)
			}
		}
		for i, constraint := range c.Constraints {
			if i > 0 || lenCols >= 1 {
				sb.WriteString(",")
			}
			if err := constraint.Restore(rsCtx); err != nil {
				return errors.Wrapf(err, "An error occurred while splicing CreateTableStmt Constraints: [%v]", i)
			}
		}
		sb.WriteString(")")
	}

	for i, option := range c.Options {
		sb.WriteString(" ")
		if err := option.Restore(rsCtx); err != nil {
			return errors.Wrapf(err, "An error occurred while splicing CreateTableStmt TableOption: [%v]", i)
		}
	}

	return nil
}

func (c *CreateTableStmt) Mode() SQLType {
	return SQLTypeCreateTable
}
