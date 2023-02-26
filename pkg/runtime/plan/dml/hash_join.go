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
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/third_party/base58"
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
	"io"
)

type HashJoinPlan struct {
	BuildPlans []proto.Plan
	ProbePlans []proto.Plan

	BuildKey []string
	ProbeKey []string
	hashArea map[string]proto.Row

	Join *ast.JoinNode
	Stmt *ast.SelectStatement
}

func (h *HashJoinPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (h *HashJoinPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := plan.Tracer.Start(ctx, "HashJoinPlan.ExecIn")
	defer span.End()

	// build stage
	buildDs, err := h.build(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// probe stage
	probeDs, err := h.probe(ctx, conn, buildDs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resultx.New(resultx.WithDataset(probeDs)), nil
}

func (h *HashJoinPlan) queryAggregate(ctx context.Context, conn proto.VConn, plans []proto.Plan) (proto.Dataset, error) {
	var generators []dataset.GenerateFunc
	for _, it := range plans {
		it := it
		generators = append(generators, func() (proto.Dataset, error) {
			res, err := it.ExecIn(ctx, conn)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return res.Dataset()
		})
	}

	ds, err := dataset.Fuse(generators[0], generators[1:]...)
	if err != nil {
		return nil, err
	}

	// todo 将所有结果聚合
	return ds, nil
}

func (h *HashJoinPlan) build(ctx context.Context, conn proto.VConn) (proto.Dataset, error) {
	ds, err := h.queryAggregate(ctx, conn, h.BuildPlans)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cn := h.BuildKey[0]
	xh := xxhash.New()
	// build map
	for {
		xh.Reset()
		next, err := ds.Next()
		if err == io.EOF {
			break
		}

		keyedRow := next.(proto.KeyedRow)
		value, err := keyedRow.Get(cn)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		_, _ = xh.WriteString(value.String())
		h.hashArea[base58.Encode(xh.Sum(nil))] = next
	}

	return ds, nil
}

func (h *HashJoinPlan) probe(ctx context.Context, conn proto.VConn, buildDs proto.Dataset) (proto.Dataset, error) {
	ds, err := h.queryAggregate(ctx, conn, h.ProbePlans)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	probeMapFunc := func(row proto.Row, columnName string) proto.Row {
		keyedRow := row.(proto.KeyedRow)
		value, _ := keyedRow.Get(columnName)
		xh := xxhash.New()
		_, _ = xh.WriteString(value.String())
		return h.hashArea[base58.Encode(xh.Sum(nil))]
	}

	cn := h.ProbeKey[0]
	filterFunc := func(row proto.Row) bool {
		findRow := probeMapFunc(row, cn)
		return findRow != nil
	}

	bFields, _ := buildDs.Fields()
	// aggregate fields
	aggregateFieldsFunc := func(fields []proto.Field) []proto.Field {
		return append(bFields, fields...)
	}

	// aggregate row
	fields, _ := ds.Fields()
	transformFunc := func(row proto.Row) (proto.Row, error) {
		dest := make([]proto.Value, len(fields))
		_ = row.Scan(dest)

		matchRow := probeMapFunc(row, cn)
		bDest := make([]proto.Value, len(bFields))
		_ = matchRow.Scan(bDest)

		return rows.NewBinaryVirtualRow(append(bFields, fields...), append(bDest, dest...)), nil
	}

	// filter match row & aggregate fields and row
	return dataset.Pipe(ds, dataset.Filter(filterFunc), dataset.Map(aggregateFieldsFunc, transformFunc)), nil
}
