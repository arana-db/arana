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
	registerOptimizeHandler(ast.SQLTypeDescribe, optimizeDescribeStatement)
}

func optimizeDescribeStatement(_ context.Context, o *optimizer) (proto.Plan, error) {
	stmt := o.stmt.(*ast.DescribeStatement)
	vts := o.rule.VTables()
	vtName := []string(stmt.Table)[0]
	ret := plan.NewDescribePlan(stmt)
	ret.BindArgs(o.args)

	if vTable, ok := vts[vtName]; ok {
		shards := rule.DatabaseTables{}
		// compute all tables
		topology := vTable.Topology()
		topology.Each(func(dbIdx, tbIdx int) bool {
			if d, t, ok := topology.Render(dbIdx, tbIdx); ok {
				shards[d] = append(shards[d], t)
			}
			return true
		})
		dbName, tblName := shards.Smallest()
		ret.Database = dbName
		ret.Table = tblName
		ret.Column = stmt.Column
	}

	return ret, nil
}
