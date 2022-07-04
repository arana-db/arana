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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

func init() {
	registerOptimizeHandler(ast.SQLTypeAlterTable, optimizeAlterTable)
}

func optimizeAlterTable(_ context.Context, o *optimizer) (proto.Plan, error) {
	var (
		stmt  = o.stmt.(*ast.AlterTableStatement)
		ret   = plan.NewAlterTablePlan(stmt)
		table = stmt.Table
		vt    *rule.VTable
		ok    bool
	)
	ret.BindArgs(o.args)

	// non-sharding update
	if vt, ok = o.rule.VTable(table.Suffix()); !ok {
		return ret, nil
	}

	//TODO alter table table or column to new name , should update sharding info

	// exit if full-scan is disabled
	if !vt.AllowFullScan() {
		return nil, errDenyFullScan
	}

	// sharding
	shards := rule.DatabaseTables{}
	topology := vt.Topology()
	topology.Each(func(dbIdx, tbIdx int) bool {
		if d, t, ok := topology.Render(dbIdx, tbIdx); ok {
			shards[d] = append(shards[d], t)
		}
		return true
	})
	ret.Shards = shards
	return ret, nil
}
