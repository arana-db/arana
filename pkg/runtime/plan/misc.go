// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package plan

import (
	"fmt"
	"strings"

	"github.com/dubbogo/arana/pkg/proto"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/misc"
	"github.com/dubbogo/arana/pkg/runtime/xxast"
)

type queryResult struct {
	proto.Rows
}

func (q queryResult) LastInsertId() (uint64, error) {
	return 0, errors.New("unsupported operation")
}

func (q queryResult) RowsAffected() (uint64, error) {
	return 0, errors.New("unsupported operation")
}

type execResult struct {
	proto.Result
}

func (e execResult) Next() proto.Row {
	return nil
}

func generateSelect(table string, stmt *xxast.SelectStatement, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SELECT ")

	for _, it := range stmt.SelectSpecs {
		sb.WriteString(string(it))
		sb.WriteByte(' ')
	}

	handleSelect(sb, stmt.Select[0])
	for i := 1; i < len(stmt.Select); i++ {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		handleSelect(sb, stmt.Select[i])
	}

	if len(table) > 0 {
		sb.WriteString(" FROM ")
		handleFrom(sb, table, stmt.From)
	}

	if where := stmt.Where; where != nil {
		sb.WriteString(" WHERE ")
		if err := handleWhere(where, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if groupBy := stmt.GroupBy; groupBy != nil {
		sb.WriteString(" GROUP BY ")

		handleGroupBy(sb, groupBy.Items[0])
		for i := 1; i < len(groupBy.Items); i++ {
			sb.WriteByte(',')
			sb.WriteByte(' ')
			handleGroupBy(sb, groupBy.Items[i])
		}

		if groupBy.RollUp {
			sb.WriteString(" WITH ROLLUP")
		}
	}

	if having := stmt.Having; having != nil {
		sb.WriteString(" HAVING ")
		if err := having.Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	writeOrderBy(sb, stmt.OrderBy, false)

	if limit := stmt.Limit; limit != nil {
		sb.WriteString(" LIMIT ")
		writeLimit(limit, sb, args)
	}

	return nil
}

func handleSelect(sb *strings.Builder, sel xxast.SelectElement) {
	switch n := sel.(type) {
	case *xxast.SelectElementAll:
		if len(n.Prefix()) > 0 {
			sb.WriteString(n.Prefix())
			sb.WriteByte('.')
		}
		sb.WriteByte('*')
	case *xxast.SelectElementColumn:
		_ = xxast.ColumnNameExpressionAtom(n.Name()).Restore(sb, nil)
	case *xxast.SelectElementFunction:
		switch fn := n.Function().(type) {
		case *xxast.Function:
			_ = fn.Restore(sb, nil)
		case *xxast.AggrFunction:
			_ = fn.Restore(sb, nil)
		case *xxast.CastFunction:
			_ = fn.Restore(sb, nil)
		case *xxast.CaseWhenElseFunction:
			_ = fn.Restore(sb, nil)
		default:
			panic("unreachable")
		}
	case *xxast.SelectElementExpr:
		_ = n.Expression().Restore(sb, nil)
	default:
		panic("unreachable")
	}

	if alias := sel.Alias(); len(alias) > 0 {
		sb.WriteString(" AS ")
		misc.Wrap(sb, '`', alias)
	}
}

func handleFrom(sb *strings.Builder, table string, from []*xxast.TableSourceNode) {
	if len(from) > 1 {
		panic("todo: multiple from")
	}
	first := from[0]

	if len(first.TableName().Prefix()) > 0 {
		misc.Wrap(sb, '`', first.TableName().Prefix())
		sb.WriteByte('.')
	}
	misc.Wrap(sb, '`', table)

	if len(first.Alias()) > 0 {
		sb.WriteString(" AS ")
		misc.Wrap(sb, '`', first.Alias())
	}
}

func handleWhere(where xxast.ExpressionNode, sb *strings.Builder, args *[]int) error {
	if err := where.Restore(sb, args); err != nil {
		return errors.Wrap(err, "failed to handle where")
	}
	return nil
}

func handleGroupBy(sb *strings.Builder, groupBy *xxast.GroupByItem) {
	_ = groupBy.Expr().Restore(sb, nil)
	if !groupBy.HasOrder() {
		return
	}
	sb.WriteByte(' ')
	if groupBy.IsOrderDesc() {
		sb.WriteString("DESC")
	} else {
		sb.WriteString("ASC")
	}
}

func writeOrderBy(sb *strings.Builder, orderBy xxast.OrderByNode, useAlias bool) {
	if len(orderBy) < 1 {
		return
	}

	sb.WriteString(" ORDER BY ")
	writeOrderByItem(sb, orderBy[0], useAlias)
	for i := 1; i < len(orderBy); i++ {
		sb.WriteByte(',')
		sb.WriteByte(' ')
		writeOrderByItem(sb, orderBy[i], useAlias)
	}
}

func writeOrderByItem(sb *strings.Builder, orderBy *xxast.OrderByItem, useAlias bool) {
	if useAlias && len(orderBy.Alias) > 0 {
		misc.Wrap(sb, '`', orderBy.Alias)
	} else {
		sb.WriteString(orderBy.String())
	}

	if orderBy.Desc {
		sb.WriteByte(' ')
		sb.WriteString("DESC")
	}
}

func writeLimit(limit *xxast.LimitNode, sb *strings.Builder, args *[]int) {
	if limit.HasOffset() {
		if limit.IsOffsetVar() {
			sb.WriteByte('?')
			*args = append(*args, int(limit.Offset()))
		} else {
			_, _ = fmt.Fprintf(sb, "%d", limit.Offset())
		}
		sb.WriteByte(',')
	}
	if limit.IsLimitVar() {
		sb.WriteByte('?')
		*args = append(*args, int(limit.Limit()))
	} else {
		_, _ = fmt.Fprintf(sb, "%d", limit.Limit())
	}
}
