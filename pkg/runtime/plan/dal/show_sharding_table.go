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
	"fmt"
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
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowShardingTable)(nil)

type ShowShardingTable struct {
	plan.BasePlan
	Stmt *ast.ShowShardingTable
	rule *rule.Rule
}

// NewShowShardingTablePlan create ShowShardingTable Plan
func NewShowShardingTablePlan(stmt *ast.ShowShardingTable) *ShowShardingTable {
	return &ShowShardingTable{
		Stmt: stmt,
	}
}

func (st *ShowShardingTable) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (st *ShowShardingTable) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		err     error
		dbName  string
	)
	ctx, span := plan.Tracer.Start(ctx, "ShowShardingTable.ExecIn")
	defer span.End()

	if err = st.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	dbName = sb.String()

	fields := thead.ShardingRule.ToFields()

	ds := &dataset.VirtualDataset{
		Columns: fields,
	}
	vtables := st.rule.VTables()
	if len(vtables) <= 0 {
		return nil, errors.New(fmt.Sprintf("%s do not have %s's sharding table", rcontext.Schema(ctx), dbName))
	}
	numShardingTable := 0
	for key, vtable := range vtables {
		rawMetadata, err := vtable.GetShardMetaDataJSON()
		if err != nil {
			return nil, errors.New(fmt.Sprintf("vtable %s json marshal failed . ", key))
		}
		vdb_name := strings.Split(rawMetadata["name"], ".")[0]
		if vdb_name != dbName {
			continue
		}
		numShardingTable += 1

		ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
			proto.NewValueString(rawMetadata["name"]), proto.NewValueString(rawMetadata["allow_full_scan"]),
			proto.NewValueString(rawMetadata["sequence_type"]), proto.NewValueString(rawMetadata["db_rules"]),
			proto.NewValueString(rawMetadata["tbl_rules"]), proto.NewValueString(rawMetadata["attributes"]),
		}))

	}
	if numShardingTable == 0 {
		return nil, errors.Errorf("%s do not have %s's sharding table", rcontext.Schema(ctx), dbName)
	}

	return resultx.New(resultx.WithDataset(ds)), nil
}

func (st *ShowShardingTable) SetRule(rule *rule.Rule) {
	st.rule = rule
}
