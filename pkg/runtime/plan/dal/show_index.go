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
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowIndexPlan)(nil)

type ShowIndexPlan struct {
	plan.BasePlan
	Stmt   *ast.ShowIndex
	Shards rule.DatabaseTables
}

func (s *ShowIndexPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowIndexPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		err     error
	)

	if s.Shards == nil {
		if err = s.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
			return nil, errors.WithStack(err)
		}
		return conn.Query(ctx, "", sb.String(), s.ToArgs(indexes)...)
	}

	db, table := s.Shards.Smallest()
	s.Stmt.TableName = ast.TableName{table}

	if err = s.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	query, err := conn.Query(ctx, db, sb.String(), s.ToArgs(indexes)...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := query.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	toTable := s.Stmt.TableName.Suffix()

	ds = dataset.Pipe(ds,
		dataset.Map(nil, func(next proto.Row) (proto.Row, error) {
			dest := make([]proto.Value, len(fields))
			if next.Scan(dest) != nil {
				return next, nil
			}
			dest[0] = proto.NewValueString(toTable)

			if next.IsBinary() {
				return rows.NewBinaryVirtualRow(fields, dest), nil
			}
			return rows.NewTextVirtualRow(fields, dest), nil
		}),
	)

	return resultx.New(resultx.WithDataset(ds)), nil
}
