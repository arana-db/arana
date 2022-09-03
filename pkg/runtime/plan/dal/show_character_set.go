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

	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/pkg/errors"
)

var _ proto.Plan = (*ShowCharacterSet)(nil)

type ShowCharacterSet struct {
	plan.BasePlan
	Stmt   *ast.ShowCharset
	Shards rule.DatabaseTables
}

// NewShowCharacterSet create ShowCreate Plan
func NewShowCharacterSetPlan(stmt *ast.ShowCharset, shards rule.DatabaseTables) *ShowCharacterSet {
	return &ShowCharacterSet{
		Stmt:   stmt,
		Shards: shards,
	}
}

func (s *ShowCharacterSet) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowCharacterSet) ExecIn(ctx context.Context, vConn proto.VConn) (proto.Result, error) {
	var (
		sb   strings.Builder
		args []int
	)

	ctx, span := plan.Tracer.Start(ctx, "ShowCharacterSet.ExecIn")
	defer span.End()

	if err := s.Stmt.Restore(ast.RestoreDefault, &sb, &args); err != nil {
		return nil, errors.Wrap(err, "failed to execute SHOW CHARACTER SET Statement")
	}

	db, _ := s.Shards.Smallest()

	if db == "" {
		return nil, errors.New("no found db")
	}

	ret, err := vConn.Query(ctx, db, sb.String(), s.ToArgs(args)...)
	if err != nil {
		return nil, err
	}

	return ret, nil
}