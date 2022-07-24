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

var (
	_ Statement = (*DropIndexStatement)(nil)
	_ Restorer  = (*DropIndexStatement)(nil)
)

type DropIndexStatement struct {
	IfExists  bool
	IndexName string
	Table     TableName
}

func (d *DropIndexStatement) CntParams() int {
	return 0
}

func (d *DropIndexStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("DROP INDEX ")
	if d.IfExists {
		sb.WriteString("IF EXISTS")
	}
	sb.WriteString(d.IndexName)
	if len(d.Table) == 0 {
		return nil
	}
	sb.WriteString(" ON ")
	return d.Table.Restore(flag, sb, args)
}

func (d *DropIndexStatement) Mode() SQLType {
	return SQLTypeDropIndex
}
