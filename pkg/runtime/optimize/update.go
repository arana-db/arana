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

package optimize

import (
	"context"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

func init() {
	registerOptimizeHandler(ast.SQLTypeUpdate, optimizeUpdate)
}

func optimizeUpdate(_ context.Context, o *optimizer) (proto.Plan, error) {
	var (
		stmt  = o.stmt.(*ast.UpdateStatement)
		table = stmt.Table
		vt    *rule.VTable
		ok    bool
	)

	// non-sharding update
	if vt, ok = o.rule.VTable(table.Suffix()); !ok {
		ret := plan.NewUpdatePlan(stmt)
		ret.BindArgs(o.args)
		return ret, nil
	}

	//check update sharding key
	for _, element := range stmt.Updated {
		if _, _, ok := vt.GetShardMetadata(element.Column.Suffix()); ok {
			return nil, errors.New("do not support update sharding key")
		}
	}

	var (
		shards   rule.DatabaseTables
		fullScan = true
		err      error
	)

	// compute shards
	if where := stmt.Where; where != nil {
		sharder := (*Sharder)(o.rule)
		if shards, fullScan, err = sharder.Shard(table, where, o.args...); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
	}

	// exit if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, errDenyFullScan
	}

	// must be empty shards (eg: update xxx set ... where 1 = 2 and uid = 1)
	if shards.IsEmpty() {
		return plan.AlwaysEmptyExecPlan{}, nil
	}

	// compute all sharding tables
	if shards.IsFullScan() {
		// init shards
		shards = rule.DatabaseTables{}
		// compute all tables
		topology := vt.Topology()
		topology.Each(func(dbIdx, tbIdx int) bool {
			if d, t, ok := topology.Render(dbIdx, tbIdx); ok {
				shards[d] = append(shards[d], t)
			}
			return true
		})
	}

	ret := plan.NewUpdatePlan(stmt)
	ret.BindArgs(o.args)
	ret.SetShards(shards)

	return ret, nil
}
