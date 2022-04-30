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
)

import (
	"github.com/pkg/errors"
)

import (
	fieldType "github.com/arana-db/arana/pkg/constants/mysql"
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

func (s *ShowDatabasesPlan) ExecIn(ctx context.Context, _ proto.VConn) (proto.Result, error) {
	tenant, ok := security.DefaultTenantManager().GetTenantOfCluster(rcontext.Schema(ctx))
	if !ok {
		return nil, tenantErr
	}

	clusters := security.DefaultTenantManager().GetClusters(tenant)
	var rows = make([]proto.Row, 0, len(clusters))

	for _, cluster := range clusters {
		encoded := mysql.PutLengthEncodedString([]byte(cluster))
		rows = append(rows, (&mysql.Row{}).Encode([]*proto.Value{{Raw: encoded, Len: len(encoded)}},
			[]proto.Field{&mysql.Field{}}, nil))
	}

	return &mysql.Result{Fields: []proto.Field{mysql.NewField("Database", fieldType.FieldTypeVarString)}, Rows: rows, AffectedRows: 0}, nil
}
