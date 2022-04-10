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
	"bytes"
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/security"
)

var tenantErr = errors.New("current db tenant not fund")

var _ proto.Plan = (*ShowDatabasesPlan)(nil)

type ShowDatabasesPlan struct {
	basePlan
	Stmt *ast.ShowDatabases
}

func (s *ShowDatabasesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowDatabasesPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
		res  proto.Result
		err  error
	)

	if err = s.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.WithStack(err)
	}

	if res, err = conn.Query(ctx, "", sb.String(), s.toArgs(args)...); err != nil {
		return nil, err
	}

	tenant, ok := security.DefaultTenantManager().GetTenantOfCluster(rcontext.Schema(ctx))
	if !ok {
		return nil, tenantErr
	}

	clusters := security.DefaultTenantManager().GetClusters(tenant)
	var rows = make([]proto.Row, 0, len(clusters))

	for _, row := range res.GetRows() {
		for _, cluster := range clusters {
			if string(bytes.TrimSpace(row.Data())) == cluster {
				rows = append(rows, row)
				break
			}
		}
	}

	return &mysql.Result{Fields: res.GetFields(), Rows: rows, AffectedRows: uint64(len(rows))}, err
}
