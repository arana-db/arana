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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/runtime/plan/dml"
)

func init() {
	optimize.Register(ast.SQLTypeUpdate, optimizeUpdate)
}

func optimizeUpdate(_ context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	var (
		stmt  = o.Stmt.(*ast.UpdateStatement)
		table = stmt.Table
		vt    *rule.VTable
		ok    bool
	)

	// non-sharding update
	if vt, ok = o.Rule.VTable(table.Suffix()); !ok {
		ret := dml.NewUpdatePlan(stmt)
		ret.BindArgs(o.Args)
		return ret, nil
	}

	// check update sharding key
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
		sharder := (*optimize.Sharder)(o.Rule)
		if shards, fullScan, err = sharder.Shard(table, where, o.Args...); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
	}

	// exit if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, optimize.ErrDenyFullScan
	}

	// must be empty shards (eg: update xxx set ... where 1 = 2 and uid = 1)
	if shards.IsEmpty() {
		return plan.AlwaysEmptyExecPlan{}, nil
	}

	// compute all sharding tables
	if shards.IsFullScan() {
		// compute all tables
		shards = vt.Topology().Enumerate()
	}

	ret := dml.NewUpdatePlan(stmt)
	ret.BindArgs(o.Args)
	ret.SetShards(shards)

	return ret, nil
}
