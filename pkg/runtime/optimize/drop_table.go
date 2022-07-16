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
	registerOptimizeHandler(ast.SQLTypeDropTable, optimizeDropTable)
}

func optimizeDropTable(_ context.Context, o *optimizer) (proto.Plan, error) {
	stmt := o.stmt.(*ast.DropTableStatement)
	//table shard
	var shards []rule.DatabaseTables
	//tables not shard
	noShardStmt := ast.NewDropTableStatement()
	for _, table := range stmt.Tables {
		shard, err := o.computeShards(*table, nil, o.args)
		if err != nil {
			return nil, err
		}
		if shard == nil {
			noShardStmt.Tables = append(noShardStmt.Tables, table)
			continue
		}
		shards = append(shards, shard)
	}

	shardPlan := plan.NewDropTablePlan(stmt)
	shardPlan.BindArgs(o.args)
	shardPlan.SetShards(shards)

	if len(noShardStmt.Tables) == 0 {
		return shardPlan, nil
	}

	noShardPlan := plan.Transparent(noShardStmt, o.args)

	return &plan.UnionPlan{
		Plans: []proto.Plan{
			noShardPlan, shardPlan,
		},
	}, nil
}
