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

package dal

import (
	"context"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/dal"
	"github.com/arana-db/arana/pkg/security"
)

func init() {
	optimize.Register(ast.SQLTypeShowTables, optimizeShowTables)
}

func optimizeShowTables(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.ShowTables)

	ret := dal.NewShowTablesPlan(stmt)
	ret.BindArgs(o.Args)

	var tables []string
	if table, ok := stmt.BaseShowWithSingleColumn.BaseShow.Like(); ok {
		var (
			tenant   = rcontext.Tenant(ctx)
			clusters = security.DefaultTenantManager().GetClusters(tenant)
		)
		for _, cluster := range clusters {
			if table == cluster {
				groups := namespace.Load(tenant, cluster).DBGroups()
				for i := 0; i < len(groups); i++ {
					tables = append(tables, groups[i])
				}
				break
			}
		}
	}

	if len(tables) != 0 {
		ret.SetTables(tables)
	} else {
		var invertedIndex map[string]string
		for logicalTable, v := range o.Rule.VTables() {
			t := v.Topology()
			t.Each(func(x, y int) bool {
				if _, phyTable, ok := t.Render(x, y); ok {
					if invertedIndex == nil {
						invertedIndex = make(map[string]string)
					}
					invertedIndex[phyTable] = logicalTable
				}
				return true
			})
		}
		ret.SetInvertedShards(invertedIndex)
	}
	return ret, nil
}
