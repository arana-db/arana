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

	uatomic "go.uber.org/atomic"

	"golang.org/x/sync/errgroup"
)

import (
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Plan = (*UpdatePlan)(nil)

// UpdatePlan represents a plan to execute sharding-update.
type UpdatePlan struct {
	basePlan
	stmt   *ast.UpdateStatement
	shards rule.DatabaseTables
}

// NewUpdatePlan creates a sharding-update plan.
func NewUpdatePlan(stmt *ast.UpdateStatement) *UpdatePlan {
	return &UpdatePlan{stmt: stmt}
}

func (up *UpdatePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (up *UpdatePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	if up.shards == nil {
		var sb strings.Builder
		if err := up.stmt.Restore(ast.RestoreDefault, &sb, nil); err != nil {
			return nil, err
		}
		return conn.Exec(ctx, "", sb.String(), up.args...)
	}

	var (
		affects = uatomic.NewUint64(0)
		cnt     = uatomic.NewUint32(0)
	)

	var g errgroup.Group

	// TODO: should wrap with tx in the future
	for k, v := range up.shards {
		// do copy for goroutine-safe
		var (
			db     = k
			tables = v
		)
		// execute concurrent for each phy database
		g.Go(func() error {
			var (
				sb   strings.Builder
				args []int
				res  proto.Result
				err  error
			)

			sb.Grow(256)

			for _, table := range tables {
				if err = up.stmt.ResetTable(table).Restore(ast.RestoreDefault, &sb, &args); err != nil {
					return errors.WithStack(err)
				}

				if res, err = conn.Exec(ctx, db, sb.String(), up.toArgs(args)...); err != nil {
					return errors.WithStack(err)
				}

				n, _ := res.RowsAffected()
				affects.Add(n)
				cnt.Inc()

				// cleanup
				if len(args) > 0 {
					args = args[:0]
				}
				sb.Reset()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	log.Debugf("sharding update success: batch=%d, affects=%d", cnt.Load(), affects.Load())

	return &mysql.Result{AffectedRows: affects.Load()}, nil
}

func (up *UpdatePlan) SetShards(shards rule.DatabaseTables) {
	up.shards = shards
}
