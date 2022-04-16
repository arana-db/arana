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
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto/rule"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*TruncatePlan)(nil)

type TruncatePlan struct {
	basePlan
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
	if s.shards == nil || s.shards.IsEmpty() {
		return &mysql.Result{AffectedRows: 0}, nil
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

			_, err := conn.Exec(ctx, db, sb.String(), s.toArgs(args)...)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			sb.Reset()
		}
	}

	return &mysql.Result{AffectedRows: 0}, nil
}

func (s *TruncatePlan) SetShards(shards rule.DatabaseTables) {
	s.shards = shards
}
