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
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type AnalyzeTablePlan struct {
	plan.BasePlan
	Stmt   *ast.AnalyzeTableStatement
	Shards rule.DatabaseTables
}

func NewAnalyzeTablePlan(stmt *ast.AnalyzeTableStatement, shards rule.DatabaseTables) *AnalyzeTablePlan {
	return &AnalyzeTablePlan{
		Stmt:   stmt,
		Shards: shards,
	}
}

// Type get plan type
func (a *AnalyzeTablePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (a *AnalyzeTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	ctx, span := plan.Tracer.Start(ctx, "AnalyzeTable.ExecIn")
	defer span.End()

	if err := a.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "failed to execute ANALYZE TABLE statement")
	}

	// currently, only implemented db0
	db, _ := a.Shards.Smallest()
	if db == "" {
		return nil, errors.New("no found db")
	}

	ret, err := conn.Query(ctx, db, sb.String(), a.ToArgs(args)...)
	if err != nil {
		return nil, err
	}

	ds, err := ret.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds = dataset.Pipe(ds, dataset.Map(nil, func(next proto.Row) (proto.Row, error) {
		dest := make([]proto.Value, len(fields))
		if next.Scan(dest) != nil {
			return next, nil
		}

		if strings.Contains(dest[0].(string), ".") {
			dbTable := strings.Split(dest[0].(string), ".")
			dbName := strings.Split(dbTable[0], "_")
			dest[0] = dbName[0] + "." + dbTable[1]
		}

		if v, ok := dest[len(dest)-1].([]byte); ok {
			dest[len(dest)-1] = string(v)
		}

		return rows.NewTextVirtualRow(fields, dest), nil
	}))

	return resultx.New(resultx.WithDataset(ds)), nil
}
