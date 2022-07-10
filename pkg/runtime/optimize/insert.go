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

package optimize

import (
	"context"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

func init() {
	registerOptimizeHandler(ast.SQLTypeInsert, optimizeInsert)
	registerOptimizeHandler(ast.SQLTypeInsertSelect, optimizeInsertSelect)
}

func optimizeInsert(ctx context.Context, o *optimizer) (proto.Plan, error) {
	ret := plan.NewSimpleInsertPlan()
	ret.BindArgs(o.args)

	var (
		stmt = o.stmt.(*ast.InsertStatement)
		vt   *rule.VTable
		ok   bool
	)

	if vt, ok = o.rule.VTable(stmt.Table().Suffix()); !ok { // insert into non-sharding table
		ret.Put("", stmt)
		return ret, nil
	}

	// TODO: handle multiple shard keys.

	bingo := -1
	// check existing shard columns
	for i, col := range stmt.Columns() {
		if _, _, ok = vt.GetShardMetadata(col); ok {
			bingo = i
			break
		}
	}

	if bingo < 0 {
		return nil, errors.Wrap(errNoShardKeyFound, "failed to insert")
	}

	//check on duplicated key update
	for _, upd := range stmt.DuplicatedUpdates() {
		if upd.Column.Suffix() == stmt.Columns()[bingo] {
			return nil, errors.New("do not support update sharding key")
		}
	}

	var (
		sharder = (*Sharder)(o.rule)
		left    = ast.ColumnNameExpressionAtom(make([]string, 1))
		filter  = &ast.PredicateExpressionNode{
			P: &ast.BinaryComparisonPredicateNode{
				Left: &ast.AtomPredicateNode{
					A: left,
				},
				Op: cmp.Ceq,
			},
		}
		slots = make(map[string]map[string][]int) // (db,table,valuesIndex)
	)

	// reset filter
	resetFilter := func(column string, value ast.ExpressionNode) {
		left[0] = column
		filter.P.(*ast.BinaryComparisonPredicateNode).Right = value.(*ast.PredicateExpressionNode).P
	}

	for i, values := range stmt.Values() {
		value := values[bingo]
		resetFilter(stmt.Columns()[bingo], value)

		shards, _, err := sharder.Shard(stmt.Table(), filter, o.args...)

		if err != nil {
			return nil, errors.WithStack(err)
		}

		if shards.Len() != 1 {
			return nil, errors.Wrap(errNoShardKeyFound, "failed to insert")
		}

		var (
			db    string
			table string
		)

		for k, v := range shards {
			db = k
			table = v[0]
			break
		}

		if _, ok = slots[db]; !ok {
			slots[db] = make(map[string][]int)
		}
		slots[db][table] = append(slots[db][table], i)
	}

	for db, slot := range slots {
		for table, indexes := range slot {
			// clone insert stmt without values
			newborn := ast.NewInsertStatement(ast.TableName{table}, stmt.Columns())
			newborn.SetFlag(stmt.Flag())
			newborn.SetDuplicatedUpdates(stmt.DuplicatedUpdates())

			// collect values with same table
			values := make([][]ast.ExpressionNode, 0, len(indexes))
			for _, i := range indexes {
				values = append(values, stmt.Values()[i])
			}
			newborn.SetValues(values)

			rewriteInsertStatement(ctx, newborn, db, table)
			ret.Put(db, newborn)
		}
	}

	return ret, nil
}

func optimizeInsertSelect(_ context.Context, o *optimizer) (proto.Plan, error) {
	stmt := o.stmt.(*ast.InsertSelectStatement)

	ret := plan.NewInsertSelectPlan()

	ret.BindArgs(o.args)

	if _, ok := o.rule.VTable(stmt.Table().Suffix()); !ok { // insert into non-sharding table
		ret.Batch[""] = stmt
		return ret, nil
	}

	// TODO: handle shard keys.

	return nil, errors.New("not support insert-select into sharding table")
}

func rewriteInsertStatement(ctx context.Context, stmt *ast.InsertStatement, db, tb string) error {
	metaData := proto.LoadSchemaLoader().Load(ctx, db, []string{tb})[tb]
	if metaData == nil || len(metaData.ColumnNames) == 0 {
		return errors.Errorf("can not get metadata for db:%s and table:%s", db, tb)
	}

	if len(metaData.ColumnNames) == len(stmt.Columns()) {
		// User had explicitly specified every value
		return nil
	}
	columnsMetadata := metaData.Columns

	for _, colName := range stmt.Columns() {
		if columnsMetadata[colName].PrimaryKey && columnsMetadata[colName].Generated {
			// User had explicitly specified auto-generated primary key column
			return nil
		}
	}

	pkColName := ""
	for name, column := range columnsMetadata {
		if column.PrimaryKey && column.Generated {
			pkColName = name
			break
		}
	}
	if len(pkColName) < 1 {
		// There's no auto-generated primary key column
		return nil
	}

	// TODO rewrite columns and add distributed primary key
	//stmt.SetColumns(append(stmt.Columns(), pkColName))
	// append value of distributed primary key
	//newValues := stmt.Values()
	//for _, newValue := range newValues {
	//	newValue = append(newValue, )
	//}
	return nil
}
