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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type CreateTablePlan struct {
	plan.BasePlan
	Stmt     *ast.CreateTableStmt
	Database string
	Tables   []string
}

func NewCreateTablePlan(
	stmt *ast.CreateTableStmt,
	db string,
	tb []string,
) *CreateTablePlan {
	return &CreateTablePlan{
		Stmt:     stmt,
		Database: db,
		Tables:   tb,
	}
}

// Type get plan type
func (c *CreateTablePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (c *CreateTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
		err  error
	)

	ctx, span := plan.Tracer.Start(ctx, "CreateTable.ExecIn")
	defer span.End()

	switch len(c.Tables) {
	case 0:
		// no table reset
		return resultx.New(), nil
	case 1:
		// single shard table
		if err := c.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
			return nil, err
		}
		if _, err = conn.Query(ctx, c.Database, sb.String(), c.ToArgs(args)...); err != nil {
			return nil, err
		}
	default:
		// multiple shard tables
		stmt := new(ast.CreateTableStmt)
		*stmt = *c.Stmt // do copy

		restore := func(table string) error {
			sb.Reset()
			if err = c.resetTable(stmt, table); err != nil {
				return err
			}
			if err = stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
				return err
			}
			if _, err = conn.Query(ctx, c.Database, sb.String(), c.ToArgs(args)...); err != nil {
				return err
			}
			return nil
		}

		for i := 0; i < len(c.Tables); i++ {
			if err := restore(c.Tables[i]); err != nil {
				return nil, err
			}
		}
	}

	return resultx.New(), nil
}

func (c *CreateTablePlan) resetTable(stmt *ast.CreateTableStmt, table string) error {
	stmt.Table = &ast.TableName{
		table,
	}

	return nil
}
