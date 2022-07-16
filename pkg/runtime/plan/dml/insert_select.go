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

package dml

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*InsertSelectPlan)(nil)

type InsertSelectPlan struct {
	plan.BasePlan
	Batch map[string]*ast.InsertSelectStatement
}

func NewInsertSelectPlan() *InsertSelectPlan {
	return &InsertSelectPlan{
		Batch: make(map[string]*ast.InsertSelectStatement),
	}
}

func (sp *InsertSelectPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (sp *InsertSelectPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		affects      uint64
		lastInsertId uint64
	)
	// TODO: consider wrap a transaction if insert into multiple databases
	// TODO: insert in parallel
	for db, insert := range sp.Batch {
		id, affected, err := sp.doInsert(ctx, conn, db, insert)
		if err != nil {
			return nil, err
		}
		affects += affected
		if id > lastInsertId {
			lastInsertId = id
		}
	}

	return resultx.New(resultx.WithLastInsertID(lastInsertId), resultx.WithRowsAffected(affects)), nil
}

func (sp *InsertSelectPlan) doInsert(ctx context.Context, conn proto.VConn, db string, stmt *ast.InsertSelectStatement) (uint64, uint64, error) {
	var (
		sb   strings.Builder
		args []int
	)

	if err := stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return 0, 0, errors.Wrap(err, "cannot restore insert statement")
	}
	res, err := conn.Exec(ctx, db, sb.String(), sp.ToArgs(args)...)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	defer resultx.Drain(res)

	id, err := res.LastInsertId()
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	return id, affected, nil
}
