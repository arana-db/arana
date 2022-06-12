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
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*LimitPlan)(nil)

type LimitPlan struct {
	UnionPlan      *UnionPlan
	OriginLimit    *ast.LimitNode
	OverwriteLimit *ast.LimitNode
}

func (limitPlan *LimitPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (limitPlan *LimitPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	if limitPlan.UnionPlan == nil {
		return nil, errors.New("limitPlan: UnionPlan is nil")
	}

	res, err := limitPlan.UnionPlan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var count int64
	ds = dataset.Pipe(ds, dataset.Filter(func(next proto.Row) bool {
		if limitPlan.OriginLimit == nil || limitPlan.OriginLimit.Limit() == 0 {
			return true
		}

		if limitPlan.OverwriteLimit == nil {
			return true
		}

		count++
		if count < limitPlan.OriginLimit.Offset() {
			return false
		}
		if count > limitPlan.OriginLimit.Offset() && count <= limitPlan.OverwriteLimit.Limit() {
			return true
		}
		return false
	}))
	return resultx.New(resultx.WithDataset(ds)), nil
}
