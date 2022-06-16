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
	"github.com/arana-db/arana/pkg/util/log"
)

// UnionPlan merges multiple query plan.
type UnionPlan struct {
	Plans []proto.Plan
}

func (u UnionPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (u UnionPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	switch u.Plans[0].Type() {
	case proto.PlanTypeQuery:
		return u.query(ctx, conn)
	case proto.PlanTypeExec:
		return u.exec(ctx, conn)
	default:
		panic("unreachable")
	}
}

func (u UnionPlan) query(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var generators []dataset.GenerateFunc
	for _, it := range u.Plans {
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
		log.Errorf("UnionPlan Fuse error:%v", err)
		return nil, err
	}
	return resultx.New(resultx.WithDataset(ds)), nil
}

func (u UnionPlan) exec(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var id, affects uint64
	for _, it := range u.Plans {
		i, n, err := u.execOne(ctx, conn, it)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		affects += n
		id += i
	}

	return resultx.New(resultx.WithLastInsertID(id), resultx.WithRowsAffected(affects)), nil
}

func (u UnionPlan) execOne(ctx context.Context, conn proto.VConn, p proto.Plan) (uint64, uint64, error) {
	res, err := p.ExecIn(ctx, conn)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	defer resultx.Drain(res)

	id, err := res.LastInsertId()

	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}

	return id, affected, nil
}
