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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/transformer"
)

type AggregatePlan struct {
	transformer.Combiner
	AggrLoader *transformer.AggrLoader
	Plan       proto.Plan
}

func (a *AggregatePlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (a *AggregatePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := Tracer.Start(ctx, "AggregatePlan.ExecIn")
	defer span.End()
	res, err := a.Plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return a.Combiner.Merge(res, a.AggrLoader)
}
