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

var (
	_ Statement = (*DescribeStatement)(nil)
	_ Statement = (*ExplainStatement)(nil)
)

// DescribeStatement represents mysql describe statement. see https://dev.mysql.com/doc/refman/8.0/en/describe.html
type DescribeStatement struct {
	table  TableName
	column string
}

// Restore implements Restorer.
func (d *DescribeStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("DESC ")
	if err := d.table.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	if len(d.column) > 0 {
		sb.WriteByte(' ')
		WriteID(sb, d.column)
	}

	return nil
}

func (d *DescribeStatement) Validate() error {
	return nil
}

func (d *DescribeStatement) CntParams() int {
	return 0
}

func (d *DescribeStatement) Mode() SQLType {
	return Squery
}

func (d *DescribeStatement) Table() TableName {
	return d.table
}

func (d *DescribeStatement) Column() (string, bool) {
	if len(d.column) > 0 {
		return d.column, true
	}
	return "", false
}

// ExplainStatement represents mysql explain statement. see https://dev.mysql.com/doc/refman/8.0/en/explain.html
type ExplainStatement struct {
	tgt Statement
}

func (e *ExplainStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("EXPLAIN ")
	if err := e.tgt.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (e *ExplainStatement) Validate() error {
	return e.tgt.Validate()
}

func (e *ExplainStatement) Target() Statement {
	return e.tgt
}

func (e *ExplainStatement) CntParams() int {
	return e.tgt.CntParams()
}

func (e *ExplainStatement) Mode() SQLType {
	return Squery
}
