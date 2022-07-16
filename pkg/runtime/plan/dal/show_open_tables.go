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
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowOpenTablesPlan)(nil)

type ShowOpenTablesPlan struct {
	plan.BasePlan
	Database       string
	Conn           proto.DB
	Stmt           *ast.ShowOpenTables
	invertedShards map[string]string // phy table name -> logical table name
}

// NewShowOpenTablesPlan create ShowTables Plan
func NewShowOpenTablesPlan(stmt *ast.ShowOpenTables) *ShowOpenTablesPlan {
	return &ShowOpenTablesPlan{
		Stmt: stmt,
	}
}

func (st *ShowOpenTablesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowOpenTablesPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	if err = st.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		query = sb.String()
		args  = st.ToArgs(indexes)
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

			if logicalTableName, ok := st.invertedShards[dest[1].(string)]; ok {
				dest[1] = logicalTableName
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

			tableName := vr.Values()[1].(string)
			if _, ok := duplicates[tableName]; ok {
				return false
			}
			duplicates[tableName] = struct{}{}
			return true
		}),
	)
	return resultx.New(resultx.WithDataset(ds)), nil
}

func (st *ShowOpenTablesPlan) SetDatabase(database string) {
	st.Database = database
}

func (st *ShowOpenTablesPlan) SetInvertedShards(m map[string]string) {
	st.invertedShards = m
}
