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
)

import (
	"github.com/pkg/errors"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/security"
)

var _ proto.Plan = (*ShowDatabasesPlan)(nil)

type ShowDatabasesPlan struct {
	basePlan
	Stmt *ast.ShowDatabases
}

func (s *ShowDatabasesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowDatabasesPlan) ExecIn(ctx context.Context, _ proto.VConn) (proto.Result, error) {
	tenant, ok := security.DefaultTenantManager().GetTenantOfCluster(rcontext.Schema(ctx))
	if !ok {
		return nil, errors.New("no tenant found in current db")
	}

	columns := []proto.Field{mysql.NewField("Database", consts.FieldTypeVarString)}
	ds := &dataset.VirtualDataset{
		Columns: columns,
	}

	for _, cluster := range security.DefaultTenantManager().GetClusters(tenant) {
		ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(columns, []proto.Value{cluster}))
	}

	return resultx.New(resultx.WithDataset(ds)), nil
}
