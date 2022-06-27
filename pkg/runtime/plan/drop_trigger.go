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
	"strings"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*DropTriggerPlan)(nil)

type DropTriggerPlan struct {
	basePlan
	Stmt *ast.DropTriggerStatement
}

func (d *DropTriggerPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (d *DropTriggerPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	if err := d.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, err
	}

	return conn.Exec(ctx, "", sb.String(), d.toArgs(args)...)
}
