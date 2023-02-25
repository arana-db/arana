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

	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"

	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowUsersFromTenantPlan)(nil)

type ShowUsersFromTenantPlan struct {
	plan.BasePlan
	Stmt *ast.ShowUsersFromTenant
	rule *rule.Rule
}

// New ShowUsersFromTenantPlan ...
func NewShowUsersFromTenantPlan(stmt *ast.ShowUsersFromTenant) *ShowUsersFromTenantPlan {
	return &ShowUsersFromTenantPlan{
		Stmt: stmt,
	}
}

func (s *ShowUsersFromTenantPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowUsersFromTenantPlan) ExecIn(ctx context.Context, _ proto.VConn) (proto.Result, error) {
	return nil, nil
}

func (st *ShowUsersFromTenantPlan) SetRule(rule *rule.Rule) {
	st.rule = rule
}
