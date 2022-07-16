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
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/dml"
)

func init() {
	optimize.Register(ast.SQLTypeInsert, optimizeInsert)
	optimize.Register(ast.SQLTypeInsertSelect, optimizeInsertSelect)
}

func optimizeInsert(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	ret := dml.NewSimpleInsertPlan()
	ret.BindArgs(o.Args)

	var (
		stmt = o.Stmt.(*ast.InsertStatement)
		vt   *rule.VTable
		ok   bool
	)

	if vt, ok = o.Rule.VTable(stmt.Table().Suffix()); !ok { // insert into non-sharding table
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
		return nil, errors.Wrap(optimize.ErrNoShardKeyFound, "failed to insert")
	}

	//check on duplicated key update
	for _, upd := range stmt.DuplicatedUpdates() {
		if upd.Column.Suffix() == stmt.Columns()[bingo] {
			return nil, errors.New("do not support update sharding key")
		}
	}

	var (
		sharder = (*optimize.Sharder)(o.Rule)
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

		shards, _, err := sharder.Shard(stmt.Table(), filter, o.Args...)

		if err != nil {
			return nil, errors.WithStack(err)
		}

		if shards.Len() != 1 {
			return nil, errors.Wrap(optimize.ErrNoShardKeyFound, "failed to insert")
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

	_, tb0, _ := vt.Topology().Smallest()

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

			rewriteInsertStatement(ctx, newborn, tb0)
			ret.Put(db, newborn)
		}
	}

	return ret, nil
}

func optimizeInsertSelect(_ context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.InsertSelectStatement)

	ret := dml.NewInsertSelectPlan()

	ret.BindArgs(o.Args)

	if _, ok := o.Rule.VTable(stmt.Table().Suffix()); !ok { // insert into non-sharding table
		ret.Batch[""] = stmt
		return ret, nil
	}

	// TODO: handle shard keys.

	return nil, errors.New("not support insert-select into sharding table")
}

func rewriteInsertStatement(ctx context.Context, stmt *ast.InsertStatement, tb string) error {
	metadatas, err := proto.LoadSchemaLoader().Load(ctx, rcontext.Schema(ctx), []string{tb})
	if err != nil {
		return errors.WithStack(err)
	}
	metadata := metadatas[tb]
	if metadata == nil || len(metadata.ColumnNames) == 0 {
		return errors.Errorf("optimize: cannot get metadata of `%s`.`%s`", rcontext.Schema(ctx), tb)
	}

	if len(metadata.ColumnNames) == len(stmt.Columns()) {
		// User had explicitly specified every value
		return nil
	}
	columnsMetadata := metadata.Columns

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
