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
	"github.com/arana-db/arana/pkg/runtime/logical"
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
		stmt      = o.Stmt.(*ast.InsertStatement)
		vt        *rule.VTable
		ok        bool
		tableName = stmt.Table
		err       error
	)

	if vt, ok = o.Rule.VTable(stmt.Table.Suffix()); !ok { // insert into non-sharding table
		ret.Put("", stmt)
		return ret, nil
	}

	bingoList := vt.GetShardColumnIndex(stmt.Columns)

	if len(bingoList) == 0 {
		return nil, errors.Wrap(optimize.ErrNoShardKeyFound, "failed to insert")
	}

	// check on duplicated key update
	for _, upd := range stmt.DuplicatedUpdates {
		for bingo := range bingoList {
			if upd.Column.Suffix() == stmt.Columns[bingo] {
				return nil, errors.New("do not support update sharding key")
			}
		}
	}

	var (
		sharder = optimize.NewXSharder(ctx, o.Rule, o.Args)
		slots   = make(map[string]map[string][]int) // (db,table,valuesIndex)
	)

	for i, values := range stmt.Values {
		var (
			shards rule.DatabaseTables
			filter ast.ExpressionNode
		)
		if len(bingoList) == 1 {
			filter = buildFilter(stmt.Columns[bingoList[0]], values[bingoList[0]])
		} else {
			filter = buildLogicalFilter(stmt.Columns, values, bingoList)
		}

		if len(o.Hints) > 0 {
			if shards, err = optimize.Hints(tableName, o.Hints, o.Rule); err != nil {
				return nil, errors.Wrap(err, "calculate hints failed")
			}
		}

		if shards == nil {
			if shards, err = sharder.SimpleShard(tableName, filter); err != nil {
				return nil, errors.WithStack(err)
			}
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

	for db, slot := range slots {
		for table, indexes := range slot {
			// clone insert stmt without values
			newborn := ast.NewInsertStatement(ast.TableName{table}, stmt.Columns)
			newborn.SetFlag(stmt.Flag())
			newborn.DuplicatedUpdates = stmt.DuplicatedUpdates
			newborn.Hint = stmt.Hint

			// collect values with same table
			values := make([][]ast.ExpressionNode, 0, len(indexes))
			for _, i := range indexes {
				values = append(values, stmt.Values[i])
			}
			newborn.Values = values

			rewriteInsertStatement(ctx, o, vt, newborn)
			ret.Put(db, newborn)
		}
	}

	return ret, nil
}

func optimizeInsertSelect(_ context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.InsertSelectStatement)

	ret := dml.NewInsertSelectPlan()

	ret.BindArgs(o.Args)

	if _, ok := o.Rule.VTable(stmt.Table.Suffix()); !ok { // insert into non-sharding table
		ret.Batch[""] = stmt
		return ret, nil
	}

	// TODO: handle shard keys.

	return nil, errors.New("not support insert-select into sharding table")
}

func getMetadata(ctx context.Context, vtab *rule.VTable) (*proto.TableMetadata, error) {
	_, tb0, _ := vtab.Topology().Smallest()
	metadatas, err := proto.LoadSchemaLoader().Load(ctx, rcontext.Schema(ctx), []string{tb0})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	metadata := metadatas[tb0]
	if metadata == nil || len(metadata.ColumnNames) == 0 {
		return nil, errors.Errorf("optimize: cannot get metadata of `%s`.`%s`", rcontext.Schema(ctx), tb0)
	}
	return metadata, nil
}

func rewriteInsertStatement(ctx context.Context, o *optimize.Optimizer, vtab *rule.VTable, stmt *ast.InsertStatement) error {
	_, tb0, _ := vtab.Topology().Smallest()
	metadatas, err := proto.LoadSchemaLoader().Load(ctx, rcontext.Schema(ctx), []string{tb0})
	if err != nil {
		return errors.WithStack(err)
	}
	metadata := metadatas[tb0]
	if metadata == nil || len(metadata.ColumnNames) == 0 {
		return errors.Errorf("optimize: cannot get metadata of `%s`.`%s`", rcontext.Schema(ctx), tb0)
	}

	if len(metadata.ColumnNames) == len(stmt.Columns) {
		// User had explicitly specified every value
		return nil
	}
	columnsMetadata := metadata.Columns

	for _, colName := range stmt.Columns {
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

	if err := createSequenceIfAbsent(ctx, vtab, metadata); err != nil {
		return err
	}

	if len(pkColName) < 1 {
		// There's no auto-generated primary key column
		return nil
	}

	mgr := proto.LoadSequenceManager()

	seq, err := mgr.GetSequence(ctx, rcontext.Tenant(ctx), rcontext.Schema(ctx), proto.BuildAutoIncrementName(vtab.Name()))
	if err != nil {
		return err
	}

	val, err := seq.Acquire(ctx)
	if err != nil {
		return err
	}

	// TODO rewrite columns and add distributed primary key
	stmt.Columns = append(stmt.Columns, pkColName)
	// append value of distributed primary key
	for i := range stmt.Values {
		stmt.Values[i] = append(stmt.Values[i], &ast.PredicateExpressionNode{
			P: &ast.AtomPredicateNode{
				A: &ast.ConstantExpressionAtom{Inner: val},
			},
		})
	}
	return nil
}

func createSequenceIfAbsent(ctx context.Context, vtab *rule.VTable, metadata *proto.TableMetadata) error {
	seqName := proto.BuildAutoIncrementName(vtab.Name())

	seq, err := proto.LoadSequenceManager().GetSequence(ctx, rcontext.Tenant(ctx), rcontext.Schema(ctx), seqName)
	if err != nil && !errors.Is(err, proto.ErrorNotFoundSequence) {
		return errors.WithStack(err)
	}

	if seq != nil {
		return nil
	}

	columns := metadata.Columns
	for i := range columns {
		if columns[i].Generated {
			autoIncr := vtab.GetAutoIncrement()

			c := proto.SequenceConfig{
				Name:   seqName,
				Type:   autoIncr.Type,
				Option: autoIncr.Option,
			}

			if _, err := proto.LoadSequenceManager().CreateSequence(ctx, rcontext.Tenant(ctx), rcontext.Schema(ctx), c); err != nil {
				return errors.WithStack(err)
			}

			break
		}
	}
	return nil
}

func buildFilter(column string, value ast.ExpressionNode) ast.ExpressionNode {
	// reset filter
	return &ast.PredicateExpressionNode{
		P: &ast.BinaryComparisonPredicateNode{
			Left: &ast.AtomPredicateNode{
				A: ast.ColumnNameExpressionAtom([]string{column}),
			},
			Op:    cmp.Ceq,
			Right: value.(*ast.PredicateExpressionNode).P,
		},
	}
}

func buildLogicalFilter(columns []string, values []ast.ExpressionNode, bingoList []int) ast.ExpressionNode {
	filter := &ast.LogicalExpressionNode{
		Op:    logical.Land,
		Left:  buildFilter(columns[bingoList[0]], values[bingoList[0]]),
		Right: buildFilter(columns[bingoList[1]], values[bingoList[1]]),
	}
	return appendLogicalFilter(columns, values, bingoList, 2, filter)
}

func appendLogicalFilter(columns []string, values []ast.ExpressionNode, bingoList []int, index int, filter ast.ExpressionNode) ast.ExpressionNode {
	if index == len(bingoList) {
		return filter
	}
	newFilter := &ast.LogicalExpressionNode{
		Op:    logical.Land,
		Left:  filter,
		Right: buildFilter(columns[bingoList[index]], values[bingoList[index]]),
	}
	appendLogicalFilter(columns, values, bingoList, index, newFilter)
	return filter
}
