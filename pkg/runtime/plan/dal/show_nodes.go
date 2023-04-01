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
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/mysql/thead"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/security"
)

var _ proto.Plan = (*ShowTopology)(nil)

type ShowNodes struct {
	Stmt *ast.ShowNodes
}

// NewShowNodesPlan create ShowNodes Plan
func NewShowNodesPlan(stmt *ast.ShowNodes) *ShowNodes {
	return &ShowNodes{
		Stmt: stmt,
	}
}

func (st *ShowNodes) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowNodes) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := plan.Tracer.Start(ctx, "ShowNodes.ExecIn")
	defer span.End()

	tenant := st.Stmt.Tenant
	if len(tenant) == 0 {
		tenant = rcontext.Tenant(ctx)
	}
	fields := thead.Nodes.ToFields()
	ds := &dataset.VirtualDataset{
		Columns: fields,
	}
	clusters := security.DefaultTenantManager().GetClusters(tenant)
	for _, cluster := range clusters {
		ns := namespace.Load(tenant, cluster)
		groups := ns.DBGroups()
		for _, group := range groups {
			dbs := ns.DBs(group)
			for _, db := range dbs {
				conn := db.NodeConn()
				ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
					proto.NewValueString(db.ID()),
					proto.NewValueString(cluster),
					proto.NewValueString(conn.Database),
					proto.NewValueString(conn.Host),
					proto.NewValueInt64(int64(conn.Port)),
					proto.NewValueString(conn.UserName),
					proto.NewValueString(conn.Weight),
					proto.NewValueString(conn.Parameters),
				}))
			}
		}
	}

	sort.Slice(ds.Rows, func(i, j int) bool {
		c := strings.Compare(
			ds.Rows[i].(rows.VirtualRow).Values()[0].String(),
			ds.Rows[j].(rows.VirtualRow).Values()[0].String(),
		)
		if c == 0 {
			c = strings.Compare(
				ds.Rows[i].(rows.VirtualRow).Values()[1].String(),
				ds.Rows[j].(rows.VirtualRow).Values()[1].String(),
			)
		}
		if c == 0 {
			c = strings.Compare(
				ds.Rows[i].(rows.VirtualRow).Values()[2].String(),
				ds.Rows[j].(rows.VirtualRow).Values()[2].String(),
			)
		}
		return c < 0
	})
	return resultx.New(resultx.WithDataset(ds)), nil
}
