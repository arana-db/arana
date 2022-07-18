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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type DropIndexPlan struct {
	plan.BasePlan
	stmt  *ast.DropIndexStatement
	shard rule.DatabaseTables
}

func NewDropIndexPlan(stmt *ast.DropIndexStatement) *DropIndexPlan {
	return &DropIndexPlan{
		stmt: stmt,
	}
}

func (d *DropIndexPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (d *DropIndexPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	for db, tables := range d.shard {
		for i := range tables {
			table := tables[i]

			stmt := new(ast.DropIndexStatement)
			stmt.Table = ast.TableName{table}
			stmt.IndexName = d.stmt.IndexName

			if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
				return nil, err
			}

			if err := d.execOne(ctx, conn, db, sb.String(), d.ToArgs(args)); err != nil {
				return nil, errors.WithStack(err)
			}

			sb.Reset()
		}
	}

	return resultx.New(), nil
}

func (d *DropIndexPlan) SetShard(shard rule.DatabaseTables) {
	d.shard = shard
}

func (d *DropIndexPlan) execOne(ctx context.Context, conn proto.VConn, db, query string, args []interface{}) error {
	res, err := conn.Exec(ctx, db, query, args...)
	if err != nil {
		return err
	}
	_, _ = res.Dataset()
	return nil
}
