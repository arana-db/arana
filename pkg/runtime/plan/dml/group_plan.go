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
	"github.com/arana-db/arana/pkg/merge"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
)

// GroupPlan TODO now only support stmt which group by items equal with order by items, such as
// `select uid, max(score) from student group by uid order by uid`
type GroupPlan struct {
	Plan       proto.Plan
	AggItems   map[int]func() merge.Aggregator
	GroupItems []dataset.OrderByItem

	OriginColumnCount int
}

func (g *GroupPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (g *GroupPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	res, err := g.Plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resultx.New(resultx.WithDataset(dataset.Pipe(ds, dataset.GroupReduce(
		g.GroupItems,
		func(fields []proto.Field) []proto.Field {
			return fields[0:g.OriginColumnCount]
		},
		func() dataset.Reducer {
			return dataset.NewGroupReducer(g.AggItems, fields, g.OriginColumnCount)
		},
	)))), nil
}
