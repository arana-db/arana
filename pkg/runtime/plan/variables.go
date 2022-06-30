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
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*ShowVariablesPlan)(nil)

type ShowVariablesPlan struct {
	basePlan
	stmt *ast.ShowVariables
}

func NewShowVariablesPlan(stmt *ast.ShowVariables) *ShowVariablesPlan {
	return &ShowVariablesPlan{stmt: stmt}
}

func (s *ShowVariablesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowVariablesPlan) ExecIn(ctx context.Context, vConn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)
	ctx, span := Tracer.Start(ctx, "ShowVariablesPlan.ExecIn")
	defer span.End()

	if err := s.stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "failed to execute DELETE statement")
	}

	ret, err := vConn.Query(ctx, "", sb.String(), s.toArgs(args)...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}
