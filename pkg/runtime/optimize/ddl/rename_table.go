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

package ddl

import (
	"context"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/ddl"
)

func init() {
	optimize.Register(ast.SQLTypeRenameTable, optimizeRenameTable)
}

func optimizeRenameTable(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	shards := rule.DatabaseTables{}
	shardsByName := make(map[string]rule.DatabaseTables)

	for _, table := range o.Rule.VTables() {
		shards = table.Topology().Enumerate()
		shardsByName[table.Name()] = shards
		break
	}

	stmt := o.Stmt.(*ast.RenameTableStatement)
	return ddl.NewRenameTablePlan(stmt, shards, shardsByName), nil
}
