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
	"database/sql"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*ShowTablesPlan)(nil)

type ShowTablesPlan struct {
	basePlan
	Database       string
	Stmt           *ast.ShowTables
	invertedShards map[string]string // phy table name -> logical table name
}

// NewShowTablesPlan create ShowTables Plan
func NewShowTablesPlan(stmt *ast.ShowTables) *ShowTablesPlan {
	return &ShowTablesPlan{
		Stmt: stmt,
	}
}

func (st *ShowTablesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowTablesPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)
	ctx, span := Tracer.Start(ctx, "ShowTablesPlan.ExecIn")
	defer span.End()

	if err = st.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		query = sb.String()
		args  = st.toArgs(indexes)
	)

	if res, err = conn.Query(ctx, st.Database, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields, _ := ds.Fields()

	// filter duplicates
	duplicates := make(map[string]struct{})

	// 1. convert to logical table name
	// 2. filter duplicated table name
	ds = dataset.Pipe(ds,
		dataset.Map(nil, func(next proto.Row) (proto.Row, error) {
			dest := make([]proto.Value, len(fields))
			if next.Scan(dest) != nil {
				return next, nil
			}
			var tableName sql.NullString
			_ = tableName.Scan(dest[0])
			dest[0] = tableName.String

			if logicalTableName, ok := st.invertedShards[tableName.String]; ok {
				dest[0] = logicalTableName
			}

			if next.IsBinary() {
				return rows.NewBinaryVirtualRow(fields, dest), nil
			}
			return rows.NewTextVirtualRow(fields, dest), nil
		}),
		dataset.Filter(func(next proto.Row) bool {
			var vr rows.VirtualRow
			switch val := next.(type) {
			case mysql.TextRow, mysql.BinaryRow:
				return true
			case rows.VirtualRow:
				vr = val
			default:
				return true
			}

			tableName := vr.Values()[0].(string)
			if _, ok := duplicates[tableName]; ok {
				return false
			}
			duplicates[tableName] = struct{}{}
			return true
		}),
	)

	return resultx.New(resultx.WithDataset(ds)), nil
}

func (st *ShowTablesPlan) SetInvertedShards(m map[string]string) {
	st.invertedShards = m
}
