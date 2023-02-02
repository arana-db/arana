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

var _ Statement = (*CheckTableStmt)(nil)

type CheckTableStmt struct {
	Tables []*TableName
}

func NewCheckTableStmt() *CheckTableStmt {
	return &CheckTableStmt{}
}

func (c *CheckTableStmt) CntParams() int {
	return 1
}

func (c *CheckTableStmt) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("CHECK TABLE ")
	for index, table := range c.Tables {
		if index != 0 {
			sb.WriteString(", ")
		}
		if err := table.Restore(flag, sb, args); err != nil {
			return errors.Wrapf(err, "an error occurred while restore AnalyzeTableStatement.Tables[%d]", index)
		}
	}
	return nil
}

func (c *CheckTableStmt) Mode() SQLType {
	return SQLTypeCheckTable
}
