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

package utility

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	constant "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type ExplainPlan struct {
	plan.BasePlan
	stmt     *ast.ExplainStatement
	dataBase string
	shards   rule.DatabaseTables
}

func NewExplainPlan(stmt *ast.ExplainStatement) *ExplainPlan {
	return &ExplainPlan{stmt: stmt}
}

func (e *ExplainPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (e *ExplainPlan) ExecIn(ctx context.Context, vConn proto.VConn) (proto.Result, error) {
	if e.shards == nil || e.shards.IsEmpty() {
		return resultx.New(), nil
	}

	var (
		sb   strings.Builder
		stmt = new(ast.ExplainStatement)
		args []int
	)

	// prepare
	sb.Grow(256)
	*stmt = *e.stmt

	//build parallel dataset
	pBuilder := dataset.NewParallelBuilder()

	for db, tables := range e.shards {
		for _, table := range tables {
			if err := e.resetTargetTable(table); err != nil {
				return nil, err
			}
			if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
				return nil, errors.Wrap(err, "failed to restore EXPLAIN statement")
			}

			res, err := e.execOne(ctx, vConn, db, sb.String(), e.ToArgs(args))
			if err != nil {
				return nil, errors.WithStack(err)
			}
			var (
				rr     = res.(*mysql.RawResult)
				fields []proto.Field
			)

			ds, err := rr.Dataset()
			if err != nil {
				return nil, errors.WithStack(err)
			}

			if fields, err = ds.Fields(); err != nil {
				return nil, errors.WithStack(err)
			}

			// add column table_name to result
			newField := append([]proto.Field{mysql.NewField("table_name", constant.FieldTypeVarString)}, fields...)
			ds = dataset.Pipe(ds,
				dataset.Map(
					func(oriField []proto.Field) []proto.Field {
						return newField
					},
					func(oriRow proto.Row) (proto.Row, error) {
						oriVal := make([]proto.Value, len(fields))
						err = oriRow.Scan(oriVal)
						if err != nil {
							return nil, err
						}
						newVal := append([]proto.Value{proto.NewValueString(table)}, oriVal...)
						if oriRow.IsBinary() {
							return rows.NewBinaryVirtualRow(newField, newVal), nil
						}
						return rows.NewTextVirtualRow(newField, newVal), nil
					}))

			// add single result to parallel ds
			vcol, _ := ds.Fields()
			vrow, err := ds.Next()
			if err == nil {
				pBuilder.Add(func() (proto.Dataset, error) {
					return &dataset.VirtualDataset{Columns: vcol, Rows: []proto.Row{vrow}}, nil
				})
			}

			// cleanup
			if len(args) > 0 {
				args = args[:0]
			}
			sb.Reset()
			rr.Discard()
		}
	}

	// parallel ds
	pDs, err := pBuilder.Build()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// order by table_name
	return resultx.New(resultx.WithDataset(dataset.NewOrderedDataset(pDs, []dataset.OrderByItem{{
		Column: "table_name",
		Desc:   false,
	}}))), nil
}

func (e *ExplainPlan) SetShards(shards rule.DatabaseTables) {
	e.shards = shards
}

func (e *ExplainPlan) resetTargetTable(table string) error {
	switch e.stmt.Target.Mode() {
	case ast.SQLTypeSelect:
		targetStmt, ok := e.stmt.Target.(*ast.SelectStatement)
		if !ok {
			return errors.New("fail to get explain target statement")
		}
		// reset table for select stmt is complicated
		targetTable := targetStmt.From[0].Source.(ast.TableName)
		targetStmt.From[0].Source = targetTable.ResetSuffix(table)
		return nil
	case ast.SQLTypeDelete:
		targetStmt, ok := e.stmt.Target.(*ast.DeleteStatement)
		if !ok {
			return errors.New("fail to get explain target statement")
		}
		targetStmt.Table = targetStmt.Table.ResetSuffix(table)
		return nil
	case ast.SQLTypeInsert:
		targetStmt, ok := e.stmt.Target.(*ast.InsertStatement)
		if !ok {
			return errors.New("fail to get explain target statement")
		}
		targetStmt.Table = targetStmt.Table.ResetSuffix(table)
		return nil
	case ast.SQLTypeUpdate:
		targetStmt, ok := e.stmt.Target.(*ast.UpdateStatement)
		if !ok {
			return errors.New("fail to get explain target statement")
		}
		targetStmt.Table = targetStmt.Table.ResetSuffix(table)
		return nil
	}
	return errors.New("no target statement found for explain statement")
}

func (e *ExplainPlan) execOne(ctx context.Context, conn proto.VConn, db, query string, args []proto.Value) (proto.Result, error) {
	return conn.Query(ctx, db, query, args...)
}
