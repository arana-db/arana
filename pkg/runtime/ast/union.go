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

const (
	_ UnionType = iota
	UnionTypeAll
	UnionTypeDistinct
)

var (
	_ Statement     = (*UnionSelectStatement)(nil)
	_ paramsCounter = (*UnionSelectStatement)(nil)
	_ Restorer      = (*UnionSelectStatement)(nil)
)

var _unionTypeNames = [...]string{
	UnionTypeAll:      "ALL",
	UnionTypeDistinct: "DISTINCT",
}

type UnionType uint8

func (u UnionType) String() string {
	return _unionTypeNames[u]
}

type UnionSelectStatement struct {
	First               *SelectStatement
	UnionStatementItems []*UnionStatementItem
	OrderBy             OrderByNode
}

func (u *UnionSelectStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := u.First.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	for _, it := range u.UnionStatementItems {
		switch it.Type {
		case UnionTypeDistinct:
			sb.WriteString(" UNION ")
		case UnionTypeAll:
			sb.WriteString(" UNION ALL ")
		default:
			panic("unreachable")
		}
		if err := it.Stmt.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if u.OrderBy != nil {
		sb.WriteString(" ORDER BY ")
		if err := u.OrderBy.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (u *UnionSelectStatement) CntParams() int {
	var cnt int

	cnt += u.First.CntParams()
	for _, it := range u.UnionStatementItems {
		cnt += it.Stmt.CntParams()
	}

	return cnt
}

func (u *UnionSelectStatement) Mode() SQLType {
	return SQLTypeUnion
}

type UnionStatementItem struct {
	Type UnionType
	Stmt *SelectStatement
}
