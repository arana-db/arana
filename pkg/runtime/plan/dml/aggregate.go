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
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/reduce"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type AggregatePlan struct {
	proto.Plan
	Fields []ast.SelectElement
}

func (ap *AggregatePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := plan.Tracer.Start(ctx, "AggregatePlan.ExecIn")
	defer span.End()

	reds, err := ap.probe()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	res, err := ap.Plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resultx.New(resultx.WithDataset(dataset.Pipe(ds, dataset.Reduce(reds)))), nil
}

func (ap *AggregatePlan) probe() (map[int]reduce.Reducer, error) {
	aggrTable := make(map[int]reduce.Reducer)
	for i, field := range ap.Fields {
		var fun ast.Node
		switch x := field.(type) {
		case *ext.MappingSelectElement:
			continue
		case ext.SelectElementProvider:
			switch y := x.Prev().(type) {
			case *ast.SelectElementFunction:
				fun = y.Function()
			}
		case *ast.SelectElementFunction:
			fun = x.Function()
		}

		aggr, ok := fun.(*ast.AggrFunction)
		if !ok {
			continue
		}

		switch aggr.Name() {
		case ast.AggrMin:
			aggrTable[i] = reduce.Min()
		case ast.AggrMax:
			aggrTable[i] = reduce.Max()
		case ast.AggrSum, ast.AggrCount:
			aggrTable[i] = reduce.Sum()
		default:
			return nil, errors.Errorf("invalid aggregate %s", aggr.Name())
		}
	}

	return aggrTable, nil
}
