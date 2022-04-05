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
	"time"
)

import (
	"github.com/pkg/errors"
)

const (
	_selectForUpdate uint8 = 1 << iota
	_selectLockInShareMode
	_selectDistinct
)

var (
	_ Statement     = (*SelectStatement)(nil)
	_ paramsCounter = (*SelectStatement)(nil)
	_ Restorer      = (*SelectStatement)(nil)
)

type SelectStatement struct {
	flag       uint8
	Select     SelectNode
	From       FromNode
	Where      ExpressionNode
	GroupBy    *GroupByNode
	Having     ExpressionNode
	OrderBy    OrderByNode
	Limit      *LimitNode
	ModifiedAt time.Time
}

func (ss *SelectStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SELECT ")

	if ss.IsDistinct() {
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

	if ss.IsForUpdate() {
		sb.WriteString(" FOR UPDATE")
	} else if ss.IsLockInShardMode() {
		sb.WriteString(" LOCK IN SHARE MODE")
	}

	return nil
}

func (ss *SelectStatement) IsDistinct() bool {
	return ss.flag&_selectDistinct != 0
}

func (ss *SelectStatement) IsLockInShardMode() bool {
	return ss.flag&_selectLockInShareMode != 0
}

func (ss *SelectStatement) IsForUpdate() bool {
	return ss.flag&_selectForUpdate != 0
}

func (ss *SelectStatement) HasJoin() bool {
	switch len(ss.From) {
	case 0:
		return false
	case 1:
		if _, ok := ss.From[0].Join(); ok {
			return true
		}

		if sub := ss.From[0].SubQuery(); sub != nil {
			switch it := sub.(type) {
			case *SelectStatement:
				return it.HasJoin()
			case *UnionSelectStatement:
				if it.first.HasJoin() {
					return true
				}
				for _, next := range it.others {
					if next.SelectStatement().HasJoin() {
						return true
					}
				}
			}
			if stmt, ok := sub.(*SelectStatement); ok {
				return stmt.HasJoin()
			}
		}

		return false
	default:
		return true
	}
}

func (ss *SelectStatement) HasSubQuery() bool {
	for _, it := range ss.From {
		if it.SubQuery() != nil {
			return true
		}
	}
	return false
}

func (ss *SelectStatement) Validate() error {
	tables := make(map[string]struct{})
	for _, it := range ss.From {
		alias := it.Alias()

		if len(alias) > 0 {
			tables[alias] = struct{}{}
		} else if tn := it.TableName(); tn != nil {
			tables[tn.Suffix()] = struct{}{}
			continue
		}

		if sq := it.SubQuery(); sq != nil {
			switch val := sq.(type) {
			case *SelectStatement:
				if err := val.Validate(); err != nil {
					return err
				}
			case *UnionSelectStatement:
				if err := val.Validate(); err != nil {
					return err
				}
			}
		}
	}

	for _, sel := range ss.Select {
		if err := sel.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid SELECT clause")
		}
	}

	if ss.Where != nil {
		if err := ss.Where.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid WHERE clause")
		}
	}

	if ss.OrderBy != nil {
		if err := ss.OrderBy.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid ORDER BY clause")
		}
	}

	if ss.GroupBy != nil {
		if err := ss.OrderBy.InTables(tables); err != nil {
			return errors.Wrap(err, "invalid GROUP BY clause")
		}
	}

	return nil
}

func (ss *SelectStatement) Mode() SQLType {
	return Squery
}

func (ss *SelectStatement) CntParams() int {
	var cnt int

	for _, it := range ss.From {
		if sq := it.SubQuery(); sq != nil {
			cnt += sq.CntParams()
		}
	}

	if ss.Where != nil {
		cnt += ss.Where.CntParams()
	}

	if ss.GroupBy != nil {
		for _, it := range ss.GroupBy.Items {
			cnt += it.Expr().CntParams()
		}
	}

	if ss.Having != nil {
		cnt += ss.Having.CntParams()
	}

	if len(ss.OrderBy) > 0 {
		for _, it := range ss.OrderBy {
			cnt += it.Expr.CntParams()
		}
	}

	if ss.Limit != nil {
		if ss.Limit.IsLimitVar() {
			cnt += 1
		}
		if ss.Limit.IsOffsetVar() {
			cnt += 1
		}
	}
	return cnt
}

func (ss *SelectStatement) enableDistinct() {
	ss.flag |= _selectDistinct
}

func (ss *SelectStatement) enableForUpdate() {
	ss.flag |= _selectForUpdate
}

func (ss *SelectStatement) enableLockInShareMode() {
	ss.flag |= _selectLockInShareMode
}
