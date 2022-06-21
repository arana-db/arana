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

package plan

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
)

type DropTablePlan struct {
	basePlan
	stmt      *ast.DropTableStatement
	shardsMap []rule.DatabaseTables
}

func NewDropTablePlan(stmt *ast.DropTableStatement) *DropTablePlan {
	return &DropTablePlan{
		stmt: stmt,
	}
}

func (d *DropTablePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (d *DropTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := Tracer.Start(ctx, "DropTablePlan.ExecIn")
	defer span.End()
	var (
		sb   strings.Builder
		args []int
	)

	for _, shards := range d.shardsMap {
		var stmt = new(ast.DropTableStatement)
		for db, tables := range shards {
			for _, table := range tables {

				stmt.Tables = append(stmt.Tables, &ast.TableName{
					table,
				})
			}
			err := stmt.Restore(ast.RestoreDefault, &sb, &args)

			if err != nil {
				return nil, err
			}

			if err = d.execOne(ctx, conn, db, sb.String(), d.toArgs(args)); err != nil {
				return nil, errors.WithStack(err)
			}

			sb.Reset()
		}
	}

	return resultx.New(), nil
}

func (d *DropTablePlan) SetShards(shardsMap []rule.DatabaseTables) {
	d.shardsMap = shardsMap
}

func (d *DropTablePlan) execOne(ctx context.Context, conn proto.VConn, db, query string, args []interface{}) error {
	res, err := conn.Exec(ctx, db, query, args...)
	if err != nil {
		return err
	}
	_, _ = res.Dataset()
	return nil
}
