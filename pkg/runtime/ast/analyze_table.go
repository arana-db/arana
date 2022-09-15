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

var _ Statement = (*AnalyzeTableStatement)(nil)

type AnalyzeTableStatement struct {
	Tables []*TableName
}

func NewAnalyzeTableStatement() *AnalyzeTableStatement {
	return &AnalyzeTableStatement{}
}

func (a *AnalyzeTableStatement) CntParams() int {
	return 0
}

func (a *AnalyzeTableStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("ANALYZE TABLE ")
	for index, table := range a.Tables {
		if index != 0 {
			sb.WriteString(", ")
		}
		if err := table.Restore(flag, sb, args); err != nil {
			return errors.Wrapf(err, "an error occurred while restore AnalyzeTableStatement.Tables[%d]", index)
		}
	}
	return nil
}

func (a *AnalyzeTableStatement) Mode() SQLType {
	return SQLTypeAnalyzeTable
}

// LogicTableToPhysicalTable If there is a sub-table, convert the corresponding logical table name into a physical table name
// example analyze table student transfer to analyze table student_0000, student_0001
