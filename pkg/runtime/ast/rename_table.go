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

var _ Statement = (*RenameTableStatement)(nil)

// RenameTableStatement represents mysql RENAME statement. see https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
type RenameTableStatement struct {
	TableToTables []*TableToTable
}

func NewRenameTableStmt() *RenameTableStatement {
	return &RenameTableStatement{}
}

func (r *RenameTableStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("RENAME TABLE ")
	for index, table := range r.TableToTables {
		if index != 0 {
			sb.WriteString(", ")
		}
		if err := table.OldTable.Restore(flag, sb, args); err != nil {
			return errors.Wrapf(err, "an error occurred while restore RenameTableStatement.TableToTables[%d].OldTable", index)
		}
		sb.WriteString(" TO ")
		if err := table.NewTable.Restore(flag, sb, args); err != nil {
			return errors.Wrapf(err, "an error occurred while restore RenameTableStatement.TableToTables[%d].NewTable", index)
		}
	}
	return nil
}

func (r *RenameTableStatement) Mode() SQLType {
	return SQLTypeRenameTable
}

func (r *RenameTableStatement) CntParams() int {
	return 0
}
