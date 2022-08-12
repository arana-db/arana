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

package dml

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
)

type selectResult struct {
	hasAggregate bool
	hasMapping   bool
	hasWeak      bool
	orders       []*ext.OrderedSelectElement
	groups       []*ext.OrderedSelectElement
}

type selectScanner struct {
	stmt *ast.SelectStatement
	args []interface{}
	sb   strings.Builder

	selectIndex      map[string]ast.SelectElement // normal selectIndex: `foo` => foo as bar
	selectAliasIndex map[string]ast.SelectElement // alias selectIndex: `bar` => foo as bar
}

func newSelectScanner(stmt *ast.SelectStatement, args []interface{}) *selectScanner {
	return &selectScanner{
		stmt: stmt,
		args: args,
	}
}

func (sc *selectScanner) scan(result *selectResult) error {
	if err := sc.probe(); err != nil {
		return errors.WithStack(err)
	}

	if err := sc.anaOrderBy(result); err != nil {
		return errors.WithStack(err)
	}

	if err := sc.anaAggregate(result); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (sc *selectScanner) probe() error {
	for i := range sc.stmt.Select {
		if err := sc.probeOne(sc.stmt.Select[i]); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (sc *selectScanner) probeOne(sel ast.SelectElement) error {
	// build selectIndex, eg: select foo as bar from ...
	var rf ast.RestoreFlag
	if alias := sel.Alias(); len(alias) > 0 {
		if sc.selectAliasIndex == nil {
			sc.selectAliasIndex = make(map[string]ast.SelectElement)
		}
		ast.WriteID(&sc.sb, alias)
		sc.selectAliasIndex[sc.sb.String()] = sel
		sc.sb.Reset()
	}
	if err := sel.Restore(rf, &sc.sb, nil); err != nil {
		return errors.WithStack(err)
	}

	if sc.selectIndex == nil {
		sc.selectIndex = make(map[string]ast.SelectElement)
	}
	sc.selectIndex[sc.sb.String()] = sel
	sc.sb.Reset()
	return nil
}

func (sc *selectScanner) anaOrderBy(dst *selectResult) error {
	if sc.stmt.OrderBy == nil {
		return nil
	}

	var rf ast.RestoreFlag
	for i, orderBy := range sc.stmt.OrderBy {
		if err := orderBy.Expr.Restore(rf, &sc.sb, nil); err != nil {
			return errors.WithStack(err)
		}
		search := sc.sb.String()
		sc.sb.Reset()

		var (
			sel ast.SelectElement
			ok  bool
		)

		// 1. order-by exists in select elements
		// 2. order-by is missing, will create and append a weak select element.
		if sel, ok = sc.indexOfSelect(search); !ok {
			sel = &ext.WeakSelectElement{
				SelectElement: sc.createSelectFromOrderBy(orderBy.Expr),
			}
			if err := sc.appendSelectElement(sel); err != nil {
				return errors.WithStack(err)
			}
			dst.hasWeak = true
		}

		dst.orders = append(dst.orders, &ext.OrderedSelectElement{
			SelectElement: sel,
			Ordinal:       i,
			Desc:          orderBy.Desc,
			OrderBy:       true,
		})
	}
	return nil
}

func (sc *selectScanner) createSelectFromOrderBy(exprAtom ast.ExpressionAtom) ast.SelectElement {
	switch it := exprAtom.(type) {
	case ast.ColumnNameExpressionAtom:
		return ast.NewSelectElementColumn(it, "")
	default:
		expr := &ast.PredicateExpressionNode{
			P: &ast.AtomPredicateNode{
				A: it,
			},
		}
		return ast.NewSelectElementExpr(expr, "")
	}
}

func (sc *selectScanner) appendSelectElement(sel ast.SelectElement) error {
	if err := sc.probeOne(sel); err != nil {
		return errors.WithStack(err)
	}
	sc.stmt.Select = append(sc.stmt.Select, sel)
	return nil
}

func (sc *selectScanner) indexOfSelect(search string) (ast.SelectElement, bool) {
	if exist, ok := sc.selectAliasIndex[search]; ok {
		return exist, true
	}
	if exist, ok := sc.selectIndex[search]; ok {
		return exist, true
	}
	return nil, false
}

func (sc *selectScanner) anaAggregate(result *selectResult) error {
	var av aggregateVisitor
	if _, err := sc.stmt.Accept(&av); err != nil {
		return errors.WithStack(err)
	}

	result.hasAggregate = len(av.aggregations) > 0
	result.hasMapping = av.hasMapping
	result.hasWeak = result.hasWeak || av.hasWeak

	return nil
}
