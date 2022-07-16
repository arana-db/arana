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

type CreateIndexPlan struct {
	basePlan
	stmt   *ast.CreateIndexStatement
	Shards rule.DatabaseTables
}

func NewCreateIndexPlan(stmt *ast.CreateIndexStatement) *CreateIndexPlan {
	return &CreateIndexPlan{
		stmt: stmt,
	}
}

func (c *CreateIndexPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (c *CreateIndexPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	for db, tables := range c.Shards {
		for i := range tables {
			table := tables[i]

			stmt := new(ast.CreateIndexStatement)
			stmt.Table = ast.TableName{table}
			stmt.IndexName = c.stmt.IndexName
			stmt.Keys = c.stmt.Keys

			if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
				return nil, err
			}

			if err := c.execOne(ctx, conn, db, sb.String(), c.toArgs(args)); err != nil {
				return nil, errors.WithStack(err)
			}

			sb.Reset()
		}
	}
	return resultx.New(), nil
}

func (c *CreateIndexPlan) SetShard(shard rule.DatabaseTables) {
	c.Shards = shard
}

func (c *CreateIndexPlan) execOne(ctx context.Context, conn proto.VConn, db, query string, args []interface{}) error {
	res, err := conn.Exec(ctx, db, query, args...)
	if err != nil {
		return err
	}
	_, _ = res.Dataset()
	return nil
}
