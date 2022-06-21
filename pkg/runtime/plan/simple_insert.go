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
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*SimpleInsertPlan)(nil)

type SimpleInsertPlan struct {
	basePlan
	batch map[string][]*ast.InsertStatement // key=db
}

func NewSimpleInsertPlan() *SimpleInsertPlan {
	return &SimpleInsertPlan{
		batch: make(map[string][]*ast.InsertStatement),
	}
}

func (sp *SimpleInsertPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (sp *SimpleInsertPlan) Put(db string, stmt *ast.InsertStatement) {
	sp.batch[db] = append(sp.batch[db], stmt)
}

func (sp *SimpleInsertPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		effected     uint64
		lastInsertId uint64
	)
	ctx, span := Tracer.Start(ctx, "SimpleInsertPlan.ExecIn")
	defer span.End()
	// TODO: consider wrap a transaction if insert into multiple databases
	// TODO: insert in parallel
	for db, inserts := range sp.batch {
		for _, insert := range inserts {
			res, err := sp.doInsert(ctx, conn, db, insert)
			if err != nil {
				return nil, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				effected += n
			}
			if id, _ := res.LastInsertId(); id > lastInsertId {
				lastInsertId = id
			}
		}
	}

	return &mysql.Result{
		AffectedRows: effected,
		InsertId:     lastInsertId,
		DataChan:     make(chan proto.Row, 1),
	}, nil
}

func (sp *SimpleInsertPlan) doInsert(ctx context.Context, conn proto.VConn, db string, stmt *ast.InsertStatement) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "cannot restore insert statement")
	}
	return conn.Exec(ctx, db, sb.String(), sp.toArgs(args)...)
}
