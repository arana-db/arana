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
	"github.com/arana-db/arana/pkg/mysql/thead"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/security"
)

var _ proto.Plan = (*ShowUsers)(nil)

type ShowUsers struct {
	plan.BasePlan
	Stmt *ast.ShowUsers
}

// New ShowUsers ...
func NewShowUsers(stmt *ast.ShowUsers) *ShowUsers {
	return &ShowUsers{
		Stmt: stmt,
	}
}

func (su *ShowUsers) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (su *ShowUsers) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		err     error
	)

	ctx, span := plan.Tracer.Start(ctx, "ShowUsers.ExecIn")
	defer span.End()

	if err = su.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	columns := thead.Database.ToFields()
	ds := &dataset.VirtualDataset{
		Columns: columns,
	}

	users, _ := security.DefaultTenantManager().GetUsers(rcontext.Tenant(ctx))
	for _, user := range users {
		ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(columns, []proto.Value{proto.NewValueString(user.Username)}))
	}

	return resultx.New(resultx.WithDataset(ds)), nil
}
