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
	consts "github.com/arana-db/arana/pkg/constants/mysql"
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

var _ proto.Plan = (*LocalSequencePlan)(nil)

type LocalSequencePlan struct {
	plan.BasePlan
	Stmt       *ast.SelectStatement
	VTs        []*rule.VTable
	ColumnList []string
}

func (s *LocalSequencePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *LocalSequencePlan) ExecIn(ctx context.Context, _ proto.VConn) (proto.Result, error) {
	_, span := plan.Tracer.Start(ctx, "LocalSequencePlan.ExecIn")

	defer span.End()
	var (
		theadLocalSelect thead.Thead
		columns          []proto.Field
		values           []proto.Value
	)

	for idx := 0; s.Stmt.From == nil && idx < len(s.Stmt.Select); idx++ {
		if seqColumn, ok := s.Stmt.Select[idx].(*ast.SelectElementColumn); ok && len(seqColumn.Name) == 2 {
			seqName, seqFunc := seqColumn.Name[0], seqColumn.Name[1]
			colName := seqColumn.Alias()
			if colName == "" {
				colName = strings.Join(seqColumn.Name, ".")
			}
			theadLocalSelect = append(theadLocalSelect, thead.Col{Name: colName, FieldType: consts.FieldTypeLong})
			seq, err := proto.LoadSequenceManager().GetSequence(ctx, rcontext.Tenant(ctx), rcontext.Schema(ctx), seqName)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			switch strings.ToLower(seqFunc) {
			case "currval":
				values = append(values, proto.NewValueInt64(seq.(proto.EnhancedSequence).CurrentVal()))
			case "nextval":
				nextSeqVal, err := seq.Acquire(ctx)
				if err != nil {
					return nil, err
				}
				values = append(values, proto.NewValueInt64(nextSeqVal))
			}
		}
	}

	columns = theadLocalSelect.ToFields()
	ds := &dataset.VirtualDataset{
		Columns: columns,
	}

	ds.Rows = append(ds.Rows, rows.NewTextVirtualRow(columns, values))
	return resultx.New(resultx.WithDataset(ds)), nil
}
