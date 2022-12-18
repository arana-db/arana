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

var _ proto.Plan = (*TruncatePlan)(nil)

type TruncatePlan struct {
	plan.BasePlan
	stmt   *ast.TruncateStatement
	shards rule.DatabaseTables
}

// NewTruncatePlan creates a truncate plan.
func NewTruncatePlan(stmt *ast.TruncateStatement) *TruncatePlan {
	return &TruncatePlan{stmt: stmt}
}

func (s *TruncatePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (s *TruncatePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := plan.Tracer.Start(ctx, "TruncatePlan.ExecIn")
	defer span.End()
	if s.shards == nil || s.shards.IsEmpty() {
		return resultx.New(), nil
	}

	var (
		sb   strings.Builder
		args []int
		stmt = new(ast.TruncateStatement)
	)

	// prepare
	sb.Grow(256)
	for db, tables := range s.shards {
		for _, table := range tables {
			stmt.Table = s.stmt.Table.ResetSuffix(table)
			if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
				return nil, errors.Wrap(err, "failed to execute TRUNCATE statement")
			}

			if err := s.execOne(ctx, conn, db, sb.String(), s.ToArgs(args)); err != nil {
				return nil, errors.WithStack(err)
			}

			sb.Reset()
		}
	}

	return resultx.New(), nil
}

func (s *TruncatePlan) SetShards(shards rule.DatabaseTables) {
	s.shards = shards
}

func (s *TruncatePlan) execOne(ctx context.Context, conn proto.VConn, db, query string, args []proto.Value) error {
	res, err := conn.Exec(ctx, db, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}

	defer resultx.Drain(res)

	return nil
}
