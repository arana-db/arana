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

var _ proto.Plan = (*SimpleDeletePlan)(nil)

// SimpleDeletePlan represents a simple delete plan for sharding table.
type SimpleDeletePlan struct {
	basePlan
	stmt   *ast.DeleteStatement
	shards rule.DatabaseTables
}

// NewSimpleDeletePlan creates a simple delete plan.
func NewSimpleDeletePlan(stmt *ast.DeleteStatement) *SimpleDeletePlan {
	return &SimpleDeletePlan{stmt: stmt}
}

func (s *SimpleDeletePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (s *SimpleDeletePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := Tracer.Start(ctx, "SimpleDeletePlan.ExecIn")
	defer span.End()
	if s.shards == nil || s.shards.IsEmpty() {
		return resultx.New(), nil
	}

	var (
		sb   strings.Builder
		stmt = new(ast.DeleteStatement)
		args []int

		affects uint64
	)

	// prepare
	sb.Grow(256)
	*stmt = *s.stmt

	// TODO: support LIMIT
	// TODO: should execute within a tx.
	for db, tables := range s.shards {
		for _, table := range tables {
			stmt.Table = s.stmt.Table.ResetSuffix(table)
			if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
				return nil, errors.Wrap(err, "failed to execute DELETE statement")
			}

			n, err := s.execOne(ctx, conn, db, sb.String(), s.toArgs(args))
			if err != nil {
				return nil, errors.WithStack(err)
			}

			affects += n

			// cleanup
			if len(args) > 0 {
				args = args[:0]
			}
			sb.Reset()
		}
	}

	return resultx.New(resultx.WithRowsAffected(affects)), nil
}

func (s *SimpleDeletePlan) SetShards(shards rule.DatabaseTables) {
	s.shards = shards
}

func (s *SimpleDeletePlan) execOne(ctx context.Context, conn proto.VConn, db, query string, args []interface{}) (uint64, error) {
	res, err := conn.Exec(ctx, db, query, args...)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer resultx.Drain(res)

	var n uint64
	if n, err = res.RowsAffected(); err != nil {
		return 0, errors.WithStack(err)
	}
	return n, nil
}
