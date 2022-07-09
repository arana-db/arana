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
	"fmt"
	"github.com/arana-db/arana/pkg/mysql/thead"
	"sort"
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
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
)

var (
	_ proto.Plan = (*ShowTopology)(nil)
)

type ShowTopology struct {
	basePlan
	Stmt *ast.ShowTopology
	rule *rule.Rule
}

// NewShowTopologyPlan create ShowTopology Plan
func NewShowTopologyPlan(stmt *ast.ShowTopology) *ShowTopology {
	return &ShowTopology{
		Stmt: stmt,
	}
}

func (st *ShowTopology) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowTopology) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		err     error
		table   string
	)
	ctx, span := Tracer.Start(ctx, "ShowTopology.ExecIn")
	defer span.End()

	if err = st.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	table = sb.String()

	fields := thead.Topology.ToFields()

	ds := &dataset.VirtualDataset{
		Columns: fields,
	}
	vtable, ok := st.rule.VTable(table)
	if !ok {
		return nil, errors.New(fmt.Sprintf("%s do not have %s's topology", rcontext.Schema(ctx), table))
	}
	t := vtable.Topology()
	t.Each(func(x, y int) bool {
		if dbGroup, phyTable, ok := t.Render(x, y); ok {
			ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
				0, dbGroup, phyTable,
			}))
		}
		return true
	})
	sort.Slice(ds.Rows, func(i, j int) bool {
		if ds.Rows[i].(rows.VirtualRow).Values()[1].(string) < ds.Rows[j].(rows.VirtualRow).Values()[1].(string) {
			return true
		}
		return ds.Rows[i].(rows.VirtualRow).Values()[1].(string) == ds.Rows[j].(rows.VirtualRow).Values()[1].(string) &&
			ds.Rows[i].(rows.VirtualRow).Values()[2].(string) < ds.Rows[j].(rows.VirtualRow).Values()[2].(string)
	})

	for id := 0; id < len(ds.Rows); id++ {
		ds.Rows[id].(rows.VirtualRow).Values()[0] = id
	}

	return resultx.New(resultx.WithDataset(ds)), nil
}

func (st *ShowTopology) SetRule(rule *rule.Rule) {
	st.rule = rule
}
