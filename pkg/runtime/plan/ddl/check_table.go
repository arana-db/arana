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

package ddl

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

type CheckTablePlan struct {
	plan.BasePlan
	Stmt         *ast.CheckTableStmt
	Shards       rule.DatabaseTables
	ShardsByName map[string]rule.DatabaseTables
}

func NewCheckTablePlan(
	stmt *ast.CheckTableStmt,
	shards rule.DatabaseTables,
	shardsByName map[string]rule.DatabaseTables,
) *CheckTablePlan {
	return &CheckTablePlan{
		Stmt:         stmt,
		Shards:       shards,
		ShardsByName: shardsByName,
	}
}

// Type get plan type
func (c *CheckTablePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (c *CheckTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	ctx, span := plan.Tracer.Start(ctx, "CheckTable.ExecIn")
	defer span.End()

	// currently, only implemented db0
	db, tb := c.Shards.Smallest()
	if db == "" {
		return nil, errors.New("no found db")
	}

	// deal partition table
	c.tableReplace(tb)

	if err := c.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "failed to execute CHECK TABLE statement")
	}

	ret, err := conn.Query(ctx, db, sb.String(), c.ToArgs(args)...)
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
		if strings.Contains(dest[0].String(), ".") {
			dbTable := strings.Split(dest[0].String(), ".")
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

			dest[0] = proto.NewValueString(dbName + "." + tbName)
		}

		return rows.NewTextVirtualRow(fields, dest), nil
	}))

	return resultx.New(resultx.WithDataset(ds)), nil
}

// tableReplace tb physical table name
func (c *CheckTablePlan) tableReplace(tb string) {
	if tb == "" {
		return
	}

	// logical to physical table map
	tableMap := c.physicalToLogicTable(tb)
	logicTb := tableMap[tb]

	stmt := ast.NewCheckTableStmt()

	for _, table := range c.Stmt.Tables {
		if strings.Trim(table.String(), "`") == logicTb {
			stmt.Tables = append(stmt.Tables, &ast.TableName{tb})
		} else {
			stmt.Tables = append(stmt.Tables, table)
		}
	}

	c.Stmt = stmt
}

// physicalToLogicTable logical to physical table map
func (c *CheckTablePlan) physicalToLogicTable(tbName string) map[string]string {
	res := make(map[string]string)

L1:
	for logicTableName, shards := range c.ShardsByName {
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
