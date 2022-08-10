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
	"sort"
	"strings"
	"sync"
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

var _ proto.Plan = (*ShowTableStatusPlan)(nil)

type ShowTableStatusPlan struct {
	plan.BasePlan
	Database string
	Stmt     *ast.ShowTableStatus
	Shards   rule.DatabaseTables
}

func (s *ShowTableStatusPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowTableStatusPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)
	ctx, span := plan.Tracer.Start(ctx, "ShowTableStatusPlan.ExecIn")
	defer span.End()

	if err := s.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		db, table string
		dbNames   []string
	)

	for k, _ := range s.Shards {
		if strings.HasPrefix(k, s.Database) {
			dbNames = append(dbNames, k)
		}
	}

	// get first database
	db, table = s.getFirstInstance(dbNames)

	if db == "" || table == "" {
		return nil, errors.New("no found db or table")
	}

	toTable := table[:strings.LastIndex(table, "_")]

	toSql := strings.ReplaceAll(sb.String(), s.Database, db)
	toSql = strings.ReplaceAll(toSql, toTable, table)

	query, err := conn.Query(ctx, "", toSql, s.ToArgs(args)...)
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

	sm := sync.Map{}

	ds = dataset.Pipe(ds, dataset.Map(nil, func(next proto.Row) (proto.Row, error) {
		dest := make([]proto.Value, len(fields))
		if next.Scan(dest) != nil {
			return next, nil
		}
		if strings.HasPrefix(dest[0].(string), toTable) {
			dest[0] = toTable
		}
		if next.IsBinary() {
			return rows.NewBinaryVirtualRow(fields, dest), nil
		}
		return rows.NewTextVirtualRow(fields, dest), nil
	}), dataset.Filter(func(next proto.Row) bool {
		dest := make([]proto.Value, len(fields))
		if next.Scan(dest) != nil {
			return false
		}
		if _, ok := sm.Load(dest[0]); ok {
			return false
		}
		sm.Store(dest[0], "")
		return true
	}))

	return resultx.New(resultx.WithDataset(ds)), nil
}

// getFirstInstance Sort by database name to get the first one
func (s *ShowTableStatusPlan) getFirstInstance(dbNames []string) (dbName string, tableName string) {
	if len(dbNames) == 0 {
		return
	}

	sort.Strings(dbNames)

	tableNames, _ := s.Shards[dbNames[0]]

	tableName = tableNames[0]
	dbName = dbNames[0]

	return
}
