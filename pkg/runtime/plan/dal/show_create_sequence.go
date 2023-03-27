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
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowCreateSequence)(nil)

type ShowCreateSequence struct {
	Stmt *ast.ShowCreateSequence
}

func NewShowCreateSequencePlan(stmt *ast.ShowCreateSequence) *ShowCreateSequence {
	return &ShowCreateSequence{
		Stmt: stmt,
	}
}

func (su *ShowCreateSequence) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (su *ShowCreateSequence) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := plan.Tracer.Start(ctx, "ShowCreateSequencePlan.ExecIn")
	defer span.End()

	tenant := su.Stmt.Tenant
	builder, ok := proto.GetSequenceSupplier(proto.BuildAutoIncrementName(tenant))
	if !ok {
		return nil, errors.Errorf("[sequence] name=%s not exist", proto.BuildAutoIncrementName(tenant))
	}
	seq := builder()

	var ds *dataset.VirtualDataset

	if seq.GetSequenceConfig().Type == "group" {
		columns := thead.GroupSequence.ToFields()
		ds = &dataset.VirtualDataset{
			Columns: columns,
		}
		val := seq.CurrentVal()
		step := seq.GetSequenceConfig().Option["step"]
		ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(columns, []proto.Value{proto.NewValueString("group"), proto.NewValueString(string(val)), proto.NewValueString(step)}))
	} else if seq.GetSequenceConfig().Type == "snowflake" {
		columns := thead.SnowflakeSequence.ToFields()
		ds = &dataset.VirtualDataset{
			Columns: columns,
		}
		workId := seq.GetSequenceConfig().Option["work_id"]
		nodeId := seq.GetSequenceConfig().Option["node_id"]
		ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(columns, []proto.Value{proto.NewValueString("snowflake"), proto.NewValueString(workId), proto.NewValueString(nodeId)}))
	}
	return resultx.New(resultx.WithDataset(ds)), nil
}
