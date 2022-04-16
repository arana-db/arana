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
	"github.com/pkg/errors"
	"strings"
)

var (
	_ Statement = (*TruncateStatement)(nil)
)

// TruncateStatement represents mysql describe statement. see https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html
type TruncateStatement struct {
	Table  TableName
}

func (stmt *TruncateStatement) ResetTable(table string) *TruncateStatement {
	ret := new(TruncateStatement)
	*ret = *stmt

	tableName := make(TableName, len(ret.Table))
	copy(tableName, ret.Table)
	tableName[len(tableName)-1] = table

	ret.Table = tableName
	return ret
}

// Restore implements Restorer.
func (stmt *TruncateStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("TRUNCATE TABLE ")
	if err := stmt.Table.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (stmt *TruncateStatement) Validate() error {
	return nil
}

func (stmt *TruncateStatement) CntParams() int {
	return 0
}

func (stmt *TruncateStatement) Mode() SQLType {
	return Struncate
}
