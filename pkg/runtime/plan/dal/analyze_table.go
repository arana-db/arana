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

package dal

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type AnalyzeTablePlan struct {
	plan.BasePlan
	Stmt         *ast.AnalyzeTableStatement
	Shards       rule.DatabaseTables
	ShardsByName map[string]rule.DatabaseTables
}

func NewAnalyzeTablePlan(
	stmt *ast.AnalyzeTableStatement,
	shards rule.DatabaseTables,
	shardsByName map[string]rule.DatabaseTables) *AnalyzeTablePlan {
	return &AnalyzeTablePlan{
		Stmt:         stmt,
		Shards:       shards,
		ShardsByName: shardsByName,
	}
}

// Type get plan type
func (a *AnalyzeTablePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (a *AnalyzeTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	ctx, span := plan.Tracer.Start(ctx, "AnalyzeTable.ExecIn")
	defer span.End()

	// currently, only implemented db0
	db, tb := a.Shards.Smallest()
	if db == "" {
		return nil, errors.New("no found db")
	}

	// deal partition table
	a.tableReplace(tb)

	if err := a.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "failed to execute ANALYZE TABLE statement")
	}

	ret, err := conn.Query(ctx, db, sb.String(), a.ToArgs(args)...)
	if err != nil {
		return nil, err
	}

	ds, err := ret.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds = dataset.Pipe(ds, dataset.Map(nil, func(next proto.Row) (proto.Row, error) {
		dest := make([]proto.Value, len(fields))
		if next.Scan(dest) != nil {
			return next, nil
		}

		// format physical database name to logic database name
		if strings.Contains(dest[0].(string), ".") {
			dbTable := strings.Split(dest[0].(string), ".")
			dbName := dbTable[0]
			dbNameIndex := strings.LastIndex(dbTable[0], "_")
			if dbNameIndex > 0 {
				dbName = dbName[:dbNameIndex]
			}

			tbName := dbTable[1]
			tbNameIndex := strings.LastIndex(dbTable[1], "_")
			if tbNameIndex > 0 {
				tbName = tbName[:tbNameIndex]
			}

			dest[0] = dbName + "." + tbName
		}

		// msg text transfer to string
		if v, ok := dest[len(dest)-1].([]byte); ok {
			dest[len(dest)-1] = string(v)
		}

		return rows.NewTextVirtualRow(fields, dest), nil
	}))

	return resultx.New(resultx.WithDataset(ds)), nil
}

// tableReplace tb physical table name
func (a *AnalyzeTablePlan) tableReplace(tb string) {
	if tb == "" {
		return
	}

	// logical to physical table map
	tableMap := a.physicalToLogicTable(tb)
	logicTb := tableMap[tb]

	stmt := ast.NewAnalyzeTableStatement()

	for _, table := range a.Stmt.Tables {
		if strings.Trim(table.String(), "`") == logicTb {
			stmt.Tables = append(stmt.Tables, &ast.TableName{tb})
		} else {
			stmt.Tables = append(stmt.Tables, table)
		}
	}

	a.Stmt = stmt
}

// physicalToLogicTable logical to physical table map
func (a *AnalyzeTablePlan) physicalToLogicTable(tbName string) map[string]string {
	res := make(map[string]string)

L1:
	for logicTableName, shards := range a.ShardsByName {
		for _, tbs := range shards {
			for _, tb := range tbs {
				if tb == tbName {
					res[tb] = logicTableName
					continue L1
				}
			}
		}
	}

	return res
}
