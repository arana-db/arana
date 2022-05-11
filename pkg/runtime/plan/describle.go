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
	"github.com/arana-db/arana/pkg/runtime/ast"
)

type DescribePlan struct {
	basePlan
	Stmt     *ast.DescribeStatement
	Database string
	Table    string
}

func NewDescribePlan(stmt *ast.DescribeStatement) *DescribePlan {
	return &DescribePlan{Stmt: stmt}
}

func (d *DescribePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (d *DescribePlan) ExecIn(ctx context.Context, vConn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	if err = d.generate(&sb, &indexes); err != nil {
		return nil, errors.Wrap(err, "failed to generate desc/describe sql")
	}

	var (
		query = sb.String()
		args  = d.toArgs(indexes)
	)

	if res, err = vConn.Query(ctx, d.Database, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}

	return res, nil
}

func (d *DescribePlan) generate(sb *strings.Builder, args *[]int) error {
	var (
		stmt = *d.Stmt
		err  error
	)

	if d.Table == "" {
		if err = d.Stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	d.resetTable(&stmt, d.Table)
	if err = stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (d *DescribePlan) resetTable(dstmt *ast.DescribeStatement, table string) {
	dstmt.Table = dstmt.Table.ResetSuffix(table)
}
