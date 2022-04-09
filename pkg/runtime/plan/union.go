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
)

// UnionPlan merges multiple query plan.
type UnionPlan struct {
	Plans []proto.Plan
}

func (u UnionPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (u UnionPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var results []proto.Result
	for _, it := range u.Plans {
		res, err := it.ExecIn(ctx, conn)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		results = append(results, res)
	}
	return compositeResult(results), nil
}

type compositeResult []proto.Result

func (c compositeResult) GetFields() []proto.Field {
	for _, it := range c {
		if ret := it.GetFields(); ret != nil {
			return ret
		}
	}
	return nil
}

func (c compositeResult) GetRows() []proto.Row {
	var rows []proto.Row
	for _, it := range c {
		rows = append(rows, it.GetRows()...)
	}
	return rows
}

func (c compositeResult) LastInsertId() (uint64, error) {
	return 0, nil
}

func (c compositeResult) RowsAffected() (uint64, error) {
	return 0, nil
}
