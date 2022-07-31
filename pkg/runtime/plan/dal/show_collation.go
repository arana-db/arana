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

var _ proto.Plan = (*ShowCollationPlan)(nil)

type ShowCollationPlan struct {
	plan.BasePlan
	Stmt *ast.ShowCollation
}

func (s *ShowCollationPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowCollationPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		err     error
	)

	if err = s.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	return conn.Query(ctx, "", sb.String(), s.ToArgs(indexes)...)
}
