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

const (
	_flagUpdateLowPriority uint8 = 1 << iota
	_flagUpdateIgnore
)

var _ Statement = (*UpdateStatement)(nil)

// UpdateStatement represents mysql update statement. see https://dev.mysql.com/doc/refman/8.0/en/update.html
type UpdateStatement struct {
	flag       uint8
	Table      TableName
	Hint       *HintNode
	TableAlias string
	Updated    []*UpdateElement
	Where      ExpressionNode
	OrderBy    OrderByNode
	Limit      *LimitNode
}

func (u *UpdateStatement) ResetTable(table string) *UpdateStatement {
	ret := new(UpdateStatement)
	*ret = *u

	tableName := make(TableName, len(ret.Table))
	copy(tableName, ret.Table)
	tableName[len(tableName)-1] = table

	ret.Table = tableName
	return ret
}

func (u *UpdateStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("UPDATE ")

	if u.Hint != nil {
		u.Hint.Restore(flag, sb, args)
	}

	if u.IsEnableLowPriority() {
		sb.WriteString("LOW_PRIORITY ")
	}
	if u.IsIgnore() {
		sb.WriteString("IGNORE ")
	}
	if err := u.Table.Restore(flag, sb, args); err != nil {
		return err
	}
	sb.WriteString(" SET ")

	if len(u.Updated) > 0 {
		if err := u.Updated[0].Restore(flag, sb, args); err != nil {
			return err
		}
		for i := 1; i < len(u.Updated); i++ {
			sb.WriteString(", ")
			if err := u.Updated[i].Restore(flag, sb, args); err != nil {
				return err
			}
		}
	}

	if u.Where != nil {
		sb.WriteString(" WHERE ")
		if err := u.Where.Restore(flag, sb, args); err != nil {
			return err
		}
	}

	if u.OrderBy != nil {
		sb.WriteString(" ORDER BY ")
		if err := u.OrderBy.Restore(flag, sb, args); err != nil {
			return err
		}
	}

	if u.Limit != nil {
		sb.WriteString(" LIMIT ")
		if err := u.Limit.Restore(flag, sb, args); err != nil {
			return err
		}
	}

	return nil
}

func (u *UpdateStatement) IsEnableLowPriority() bool {
	return u.flag&_flagUpdateLowPriority != 0
}

func (u *UpdateStatement) IsIgnore() bool {
	return u.flag&_flagUpdateIgnore != 0
}

func (u *UpdateStatement) enableLowPriority() {
	u.flag |= _flagUpdateLowPriority
}

func (u *UpdateStatement) enableIgnore() {
	u.flag |= _flagUpdateIgnore
}

func (u *UpdateStatement) Mode() SQLType {
	return SQLTypeUpdate
}
