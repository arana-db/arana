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
	"bytes"
	"context"
	"io"
)

import (
	"github.com/cespare/xxhash/v2"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/third_party/base58"
)

type HashJoinPlan struct {
	BuildPlan proto.Plan
	ProbePlan proto.Plan

	BuildKey         string
	ProbeKey         string
	hashArea         map[string]proto.Row
	IsFilterProbeRow bool

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

func (h *HashJoinPlan) queryAggregate(ctx context.Context, conn proto.VConn, plan proto.Plan) (proto.Result, error) {
	result, err := plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (h *HashJoinPlan) build(ctx context.Context, conn proto.VConn) (proto.Dataset, error) {
	res, err := h.queryAggregate(ctx, conn, h.BuildPlan)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cn := h.BuildKey
	xh := xxhash.New()
	h.hashArea = make(map[string]proto.Row)
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

		if value != nil {
			_, _ = xh.WriteString(value.String())
			h.hashArea[base58.Encode(xh.Sum(nil))] = next
		}
	}

	return ds, nil
}

func (h *HashJoinPlan) probe(ctx context.Context, conn proto.VConn, buildDataset proto.Dataset) (proto.Dataset, error) {
	res, err := h.queryAggregate(ctx, conn, h.ProbePlan)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	probeMapFunc := func(row proto.Row, columnName string) proto.Row {
		keyedRow := row.(proto.KeyedRow)
		value, _ := keyedRow.Get(columnName)
		if value != nil {
			xh := xxhash.New()
			_, _ = xh.WriteString(value.String())
			return h.hashArea[base58.Encode(xh.Sum(nil))]
		}
		return nil
	}

	cn := h.ProbeKey
	filterFunc := func(row proto.Row) bool {
		findRow := probeMapFunc(row, cn)
		if !h.IsFilterProbeRow {
			return true
		}

		return findRow != nil
	}

	buildFields, err := buildDataset.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// aggregate fields
	aggregateFieldsFunc := func(fields []proto.Field) []proto.Field {
		return append(buildFields[:len(buildFields)-1], fields[:len(fields)-1]...)
	}

	// aggregate row
	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	transformFunc := func(row proto.Row) (proto.Row, error) {
		dest := make([]proto.Value, len(fields))
		_ = row.Scan(dest)

		matchRow := probeMapFunc(row, cn)
		buildDest := make([]proto.Value, len(buildFields))
		if matchRow != nil {
			_ = matchRow.Scan(buildDest)
		} else {
			// set null row
			if row.IsBinary() {
				matchRow = rows.NewBinaryVirtualRow(buildFields, buildDest)
			} else {
				matchRow = rows.NewTextVirtualRow(buildFields, buildDest)
			}
		}

		// remove 'ON' column
		resFields := append(buildFields[:len(buildFields)-1], fields[:len(fields)-1]...)
		resDest := append(buildDest[:len(buildDest)-1], dest[:len(dest)-1]...)

		var b bytes.Buffer
		if row.IsBinary() {
			newRow := rows.NewBinaryVirtualRow(resFields, resDest)
			_, err := newRow.WriteTo(&b)
			if err != nil {
				return nil, err
			}

			br := mysql.NewBinaryRow(resFields, b.Bytes())
			return br, nil
		} else {
			newRow := rows.NewTextVirtualRow(resFields, resDest)
			_, err := newRow.WriteTo(&b)
			if err != nil {
				return nil, err
			}

			return mysql.NewTextRow(resFields, b.Bytes()), nil
		}
	}

	// filter match row & aggregate fields and row
	return dataset.Pipe(ds, dataset.Filter(filterFunc), dataset.Map(aggregateFieldsFunc, transformFunc)), nil
}
