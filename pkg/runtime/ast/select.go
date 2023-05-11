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
	_ Statement = (*SelectStatement)(nil)
	_ Node      = (*SelectStatement)(nil)
	_ Restorer  = (*SelectStatement)(nil)
)

const (
	_ SelectLock = 1 << iota
	SelectLockForUpdate
	SelectLockInShardMode
)

var _selectLockNames = [...]string{
	SelectLockForUpdate:   "FOR UPDATE",
	SelectLockInShardMode: "LOCK IN SHARE MODE",
}

type SelectLock uint8

func (sl SelectLock) String() string {
	return _selectLockNames[sl]
}

type SelectStatement struct {
	Select   SelectNode
	Hint     *HintNode
	From     FromNode
	Where    ExpressionNode
	GroupBy  *GroupByNode
	Having   ExpressionNode
	OrderBy  OrderByNode
	Limit    *LimitNode
	Lock     SelectLock
	Distinct bool
	Hint     *HintNode
}

func (ss *SelectStatement) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitSelectStatement(ss)
}

func (ss *SelectStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SELECT ")

	if ss.Hint != nil {
		if err := ss.Hint.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		sb.WriteString(" ")
	}

	if ss.Distinct {
		sb.WriteString(Distinct)
		sb.WriteString(" ")
	}

	if err := ss.Select[0].Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	for i := 1; i < len(ss.Select); i++ {
		sb.WriteByte(',')
		if err := ss.Select[i].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if len(ss.From) > 0 {
		sb.WriteString(" FROM ")
		if err := ss.From[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		for i := 1; i < len(ss.From); i++ {
			sb.WriteString(", ")
			if err := ss.From[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	if ss.Where != nil {
		sb.WriteString(" WHERE ")
		if err := ss.Where.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if ss.GroupBy != nil {
		sb.WriteString(" GROUP BY ")

		if err := ss.GroupBy.Items[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}

		for i := 1; i < len(ss.GroupBy.Items); i++ {
			sb.WriteByte(',')
			if err := ss.GroupBy.Items[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}

		if ss.GroupBy.RollUp {
			sb.WriteString(" WITH ROLLUP ")
		}
	}

	if ss.Having != nil {
		sb.WriteString(" HAVING ")
		if err := ss.Having.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if len(ss.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		if err := ss.OrderBy[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		for i := 1; i < len(ss.OrderBy); i++ {
			sb.WriteString(", ")
			if err := ss.OrderBy[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	if ss.Limit != nil {
		sb.WriteString(" LIMIT ")
		if err := ss.Limit.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if ss.Lock != 0 {
		sb.WriteByte(' ')
		sb.WriteString(ss.Lock.String())
	}

	return nil
}

func (ss *SelectStatement) HasJoin() bool {
	switch len(ss.From) {
	case 0:
		return false
	case 1:
		if len(ss.From[0].Joins) > 0 {
			return true
		}
		switch it := ss.From[0].Source.(type) {
		case *SelectStatement:
			return it.HasJoin()
		case *UnionSelectStatement:
			if it.First.HasJoin() {
				return true
			}
			for _, next := range it.UnionStatementItems {
				if next.Stmt.HasJoin() {
					return true
				}
			}
		}
		return false
	default:
		return true
	}
}

func (ss *SelectStatement) HasSubQuery() bool {
	for _, it := range ss.From {
		switch it.Source.(type) {
		case *SelectStatement, *UnionSelectStatement:
			return true
		}
	}
	return false
}

func (ss *SelectStatement) Mode() SQLType {
	return SQLTypeSelect
}
