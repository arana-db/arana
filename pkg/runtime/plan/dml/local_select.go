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
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"strings"
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

var _ proto.Plan = (*LocalSelectPlan)(nil)

type LocalSelectPlan struct {
	plan.BasePlan
	Stmt       *ast.SelectStatement
	Result     []proto.Value
	ColumnList []string
}

func (s *LocalSelectPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *LocalSelectPlan) ExecIn(ctx context.Context, _ proto.VConn) (proto.Result, error) {
	_, span := plan.Tracer.Start(ctx, "LocalSelectPlan.ExecIn")
	defer span.End()
	var theadLocalSelect thead.Thead

	for i, item := range s.ColumnList {
		sRes := s.Result[i].String()
		if strings.ContainsRune(sRes, '.') {
			theadLocalSelect = append(theadLocalSelect, thead.Col{Name: item, FieldType: consts.FieldTypeFloat})
		} else {
			theadLocalSelect = append(theadLocalSelect, thead.Col{Name: item, FieldType: consts.FieldTypeLong})
		}
	}

	columns := theadLocalSelect.ToFields()
	ds := &dataset.VirtualDataset{
		Columns: columns,
	}

	ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(columns, s.Result))

	return resultx.New(resultx.WithDataset(ds)), nil
}
