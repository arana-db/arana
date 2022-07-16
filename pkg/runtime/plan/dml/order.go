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
	"github.com/arana-db/arana/pkg/resultx"
)

var _ proto.Plan = (*OrderPlan)(nil)

type OrderPlan struct {
	ParentPlan   proto.Plan
	OrderByItems []dataset.OrderByItem
}

func (orderPlan *OrderPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (orderPlan *OrderPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	if orderPlan.ParentPlan == nil {
		return nil, errors.New("order plan: ParentPlan is nil")
	}

	res, err := orderPlan.ParentPlan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fuseable, ok := ds.(*dataset.FuseableDataset)

	if !ok {
		return nil, errors.New("order plan convert Dataset to FuseableDataset cause error")
	}

	orderedDataset := dataset.NewOrderedDataset(fuseable.ToParallel(), orderPlan.OrderByItems)

	return resultx.New(resultx.WithDataset(orderedDataset)), nil
}
