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
	rast "github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*TransparentPlan)(nil)

// TransparentPlan represents a transparent plan.
type TransparentPlan struct {
	basePlan
	stmt rast.Statement
	db   string
	typ  proto.PlanType
}

// Transparent creates a plan which will be executed by upstream db transparently.
func Transparent(stmt rast.Statement, args []interface{}) *TransparentPlan {
	var typ proto.PlanType
	switch stmt.Mode() {
	case rast.Sinsert, rast.Sdelete, rast.Sreplace, rast.Supdate, rast.Struncate:
		typ = proto.PlanTypeExec
	default:
		typ = proto.PlanTypeQuery
	}

	tp := &TransparentPlan{stmt: stmt}
	tp.SetType(typ)
	tp.BindArgs(args)

	return tp
}

func (tp *TransparentPlan) SetDB(db string) {
	tp.db = db
}

func (tp *TransparentPlan) SetType(typ proto.PlanType) {
	tp.typ = typ
}

func (tp *TransparentPlan) Type() proto.PlanType {
	return tp.typ
}

func (tp *TransparentPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
		err  error
	)

	if err = tp.stmt.Restore(rast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.WithStack(err)
	}

	switch tp.typ {
	case proto.PlanTypeQuery:
		return conn.Query(ctx, tp.db, sb.String(), tp.toArgs(args)...)
	case proto.PlanTypeExec:
		return conn.Exec(ctx, tp.db, sb.String(), tp.toArgs(args)...)
	default:
		panic("unreachable")
	}
}
