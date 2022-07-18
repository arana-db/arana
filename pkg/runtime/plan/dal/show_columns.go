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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowColumnsPlan)(nil)

type ShowColumnsPlan struct {
	plan.BasePlan
	Stmt  *ast.ShowColumns
	Table string
}

func (s *ShowColumnsPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowColumnsPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
	)

	if err := s.generate(&sb, &indexes); err != nil {
		return nil, errors.Wrap(err, "failed to generate show columns sql")
	}

	return conn.Query(ctx, "", sb.String(), s.ToArgs(indexes)...)
}

func (s *ShowColumnsPlan) generate(sb *strings.Builder, args *[]int) error {
	var (
		stmt = *s.Stmt
		err  error
	)

	if s.Table == "" {
		if err = s.Stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	s.resetTable(&stmt, s.Table)
	if err = stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *ShowColumnsPlan) resetTable(dstmt *ast.ShowColumns, table string) {
	dstmt.TableName = dstmt.TableName.ResetSuffix(table)
}
