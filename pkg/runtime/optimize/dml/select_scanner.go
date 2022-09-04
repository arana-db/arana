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
	"reflect"
	"strings"
)

import (
	"github.com/cespare/xxhash/v2"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
	"github.com/arana-db/arana/third_party/base58"
)

const _autoPrefix = "__arana_"

type selectResult struct {
	hasAggregate     bool
	hasMapping       bool
	hasWeak          bool
	orders           []*ext.OrderedSelectElement
	groups           []*ext.OrderedSelectElement
	normalizedFields []string
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
	for _, next := range sc.stmt.Select {
		result.normalizedFields = append(result.normalizedFields, next.DisplayName())
	}

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

	var (
		rf ast.RestoreFlag
		xh *xxhash.Digest
	)
	for i, orderBy := range sc.stmt.OrderBy {
		if err := orderBy.Expr.Restore(rf, &sc.sb, nil); err != nil {
			return errors.WithStack(err)
		}
		search := sc.sb.String()
		sc.sb.Reset()

		genAlias := func() string {
			if xh == nil {
				xh = xxhash.New()
			} else {
				xh.Reset()
			}

			writeAutoAlias(xh, &sc.sb, search)

			s := sc.sb.String()
			sc.sb.Reset()
			return s
		}

		sel, isAlias, ok := sc.indexOfSelect(search)

		// 1. order-by exists in select elements
		// 2. order-by is missing, will create and append a weak select element.
		if !ok {
			switch expr := orderBy.Expr.(type) {
			case ast.ColumnNameExpressionAtom:
				// order by some_column => select ..., some_column from ...
				sel = &ext.WeakSelectElement{
					SelectElement: sc.createSelectFromOrderBy(expr, ""),
				}
			default:
				// select id,uid,name from student where ... order by 2022-year
				//    => select id,uid,name,2022-birth_year as weak_field from student where ... order by weak_field
				newAlias := genAlias()
				orderBy.Expr = ast.NewSingleColumnNameExpressionAtom(newAlias)

				sel = &ext.WeakSelectElement{
					SelectElement: sc.createSelectFromOrderBy(expr, newAlias),
				}
			}

			if err := sc.appendSelectElement(sel); err != nil {
				return errors.WithStack(err)
			}
			dst.hasWeak = true
		} else if !isAlias {
			if len(sel.Alias()) > 0 {
				// select 2022-birth_year as age from student ... order by age
				orderBy.Expr = ast.NewSingleColumnNameExpressionAtom(sel.Alias())
			} else if _, isColumn := orderBy.Expr.(ast.ColumnNameExpressionAtom); !isColumn {
				// select 2022-birth_year from student ... order by 2022-birth_year
				// 1. select 2022-birth_year as fake_alias from student ... order by fake_alias
				// 2. rename fake_alias to 2022-birth_year
				newAlias := genAlias()
				orderBy.Expr = ast.NewSingleColumnNameExpressionAtom(newAlias)

				replaceIndex := -1
				for j := 0; j < len(sc.stmt.Select); j++ {
					if reflect.DeepEqual(sc.stmt.Select[j], sel) {
						replaceIndex = j
						break
					}
				}

				sel = &ext.WeakAliasSelectElement{
					SelectElement: sel,
					WeakAlias:     newAlias,
				}

				if replaceIndex != -1 {
					sc.stmt.Select[replaceIndex] = sel
				}
			}
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

func (sc *selectScanner) createSelectFromOrderBy(exprAtom ast.ExpressionAtom, alias string) ast.SelectElement {
	switch it := exprAtom.(type) {
	case ast.ColumnNameExpressionAtom:
		return ast.NewSelectElementColumn(it, alias)
	default:
		expr := &ast.PredicateExpressionNode{
			P: &ast.AtomPredicateNode{
				A: it,
			},
		}
		return ast.NewSelectElementExpr(expr, alias)
	}
}

func (sc *selectScanner) appendSelectElement(sel ast.SelectElement) error {
	if err := sc.probeOne(sel); err != nil {
		return errors.WithStack(err)
	}
	sc.stmt.Select = append(sc.stmt.Select, sel)
	return nil
}

func (sc *selectScanner) indexOfSelect(search string) (ret ast.SelectElement, isAlias, ok bool) {
	if ret, ok = sc.selectAliasIndex[search]; ok {
		isAlias = true
		return
	}
	if ret, ok = sc.selectIndex[search]; ok {
		return
	}
	return
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

func writeAutoAlias(xh *xxhash.Digest, sb *strings.Builder, name string) {
	_, _ = xh.WriteString(name)
	sb.WriteString(_autoPrefix)
	sb.WriteString(base58.Encode(xh.Sum(nil)))
}
