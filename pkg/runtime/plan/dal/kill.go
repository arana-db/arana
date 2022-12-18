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

type KillPlan struct {
	plan.BasePlan
	db   string
	Stmt *ast.KillStmt
}

func (k *KillPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (k *KillPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var sb strings.Builder
	ctx, span := plan.Tracer.Start(ctx, "KillPlan.ExecIn")
	defer span.End()

	if err := k.Stmt.Restore(ast.RestoreDefault, &sb, nil); err != nil {
		return nil, errors.WithStack(err)
	}
	return conn.Exec(ctx, k.db, sb.String())
}

func (k *KillPlan) SetDatabase(db string) {
	k.db = db
}

func NewKillPlan(stmt *ast.KillStmt) *KillPlan {
	return &KillPlan{
		Stmt: stmt,
	}
}
