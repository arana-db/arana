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
 *
 */

package plan

import (
	"context"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/pkg/errors"
)

var tenantErr = errors.New("current db tenant not fund")

var _ proto.Plan = (*ShowVariablesPlan)(nil)

type ShowVariablesPlan struct {
	basePlan
	Stmt *ast.ShowVariables
}

func (s *ShowVariablesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowVariablesPlan) ExecIn(ctx context.Context, vConn proto.VConn) (proto.Result, error) {

	ret, err := vConn.Exec(ctx, "", rcontext.SQL(ctx))
	if err != nil {
		return nil, err
	}

	return &mysql.Result{
		Rows: ret.GetRows(),
	}, nil
}
