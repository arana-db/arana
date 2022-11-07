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
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*ShowReplicasPlan)(nil)

type ShowReplicasPlan struct {
	plan.BasePlan
	Stmt *ast.ShowReplicas
}

func (s *ShowReplicasPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func NewShowReplicasPlan(stmt *ast.ShowReplicas) *ShowReplicasPlan {
	return &ShowReplicasPlan{Stmt: stmt}
}

func (s *ShowReplicasPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {

	var (
		sb   strings.Builder
		args []int
	)
	ctx, span := plan.Tracer.Start(ctx, "ShowReplicasPlan.ExecIn")
	defer span.End()

	if err := s.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "failed to execute SHOW REPLICAS statement")
	}

	ret, err := conn.Query(ctx, "", sb.String(), s.ToArgs(args)...)
	if err != nil {
		return nil, err
	}

	return ret, nil

}
