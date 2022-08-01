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

var _ Statement = (*DeleteStatement)(nil)

const (
	_deleteLowPriority uint8 = 1 << iota
	_deleteQuick
	_deleteIgnore
)

// DeleteStatement represents mysql delete statement. see https://dev.mysql.com/doc/refman/8.0/en/delete.html
type DeleteStatement struct {
	flag    uint8
	Table   TableName
	Where   ExpressionNode
	OrderBy OrderByNode
	Limit   *LimitNode
}

// Restore implements Restorer.
func (ds *DeleteStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("DELETE ")

	if ds.IsLowPriority() {
		sb.WriteString("LOW_PRIORITY ")
	}

	if ds.IsQuick() {
		sb.WriteString("QUICK ")
	}

	if ds.IsIgnore() {
		sb.WriteString("IGNORE ")
	}

	sb.WriteString("FROM ")

	if err := ds.Table.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	// TODO: partitions

	if ds.Where != nil {
		sb.WriteString(" WHERE ")
		if err := ds.Where.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if ds.OrderBy != nil {
		sb.WriteString(" ORDER BY ")
		if err := ds.OrderBy.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if ds.Limit != nil {
		sb.WriteString(" LIMIT ")
		if err := ds.Limit.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (ds *DeleteStatement) CntParams() int {
	var n int
	if ds.Where != nil {
		n += ds.Where.CntParams()
	}
	if limit := ds.Limit; limit != nil && limit.IsLimitVar() {
		n++
	}
	return n
}

func (ds *DeleteStatement) Mode() SQLType {
	return SQLTypeDelete
}

func (ds *DeleteStatement) IsLowPriority() bool {
	return ds.flag&_deleteLowPriority != 0
}

func (ds *DeleteStatement) IsQuick() bool {
	return ds.flag&_deleteQuick != 0
}

func (ds *DeleteStatement) IsIgnore() bool {
	return ds.flag&_deleteIgnore != 0
}

func (ds *DeleteStatement) enableLowPriority() {
	ds.flag |= _deleteLowPriority
}

func (ds *DeleteStatement) enableQuick() {
	ds.flag |= _deleteQuick
}

func (ds *DeleteStatement) enableIgnore() {
	ds.flag |= _deleteIgnore
}
