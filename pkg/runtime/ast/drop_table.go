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
	"github.com/pingcap/errors"
	"strings"
)

type DropTableStatement struct {
	Tables []*TableName
}

func NewDropTableStatement() *DropTableStatement {
	return &DropTableStatement{
		Tables: []*TableName{},
	}
}

func (d DropTableStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("DROP TABLE ")
	for index, table := range d.Tables {
		if index != 0 {
			sb.WriteString(", ")
		}
		if err := table.Restore(flag, sb, args); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropTableStatement.Tables[%d]", index)
		}
	}
	return nil
}

func (d DropTableStatement) CntParams() int {
	return 0
}

func (d DropTableStatement) Validate() error {
	return nil
}

func (d DropTableStatement) Mode() SQLType {
	return SdropTable
}
