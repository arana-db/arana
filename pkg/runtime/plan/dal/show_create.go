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
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowCreatePlan)(nil)

type ShowCreatePlan struct {
	plan.BasePlan
	Stmt     *ast.ShowCreate
	Database string
	Table    string
}

// NewShowCreatePlan create ShowCreate Plan
func NewShowCreatePlan(stmt *ast.ShowCreate) *ShowCreatePlan {
	return &ShowCreatePlan{
		Stmt: stmt,
	}
}

func (st *ShowCreatePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowCreatePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	if err = st.Stmt.ResetTable(st.Table).Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		query  = sb.String()
		args   = st.ToArgs(indexes)
		target = st.Stmt.Target()
	)

	if res, err = conn.Query(ctx, st.Database, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// sharding table should be changed of target name
	if st.Table != target {
		fields, _ := ds.Fields()
		ds = dataset.Pipe(ds,
			dataset.Map(nil, func(next proto.Row) (proto.Row, error) {
				dest := make([]proto.Value, len(fields))
				if next.Scan(dest) != nil {
					return next, nil
				}
				dest[0] = target
				dest[1] = strings.Replace(dest[1].(string), st.Table, target, 1)

				if next.IsBinary() {
					return rows.NewBinaryVirtualRow(fields, dest), nil
				}
				return rows.NewTextVirtualRow(fields, dest), nil
			}),
		)
	}

	return resultx.New(resultx.WithDataset(ds)), nil
}
