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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Plan = (*AlterTablePlan)(nil)

type AlterTablePlan struct {
	basePlan
	stmt   *ast.AlterTableStatement
	Shards rule.DatabaseTables
}

func NewAlterTablePlan(stmt *ast.AlterTableStatement) *AlterTablePlan {
	return &AlterTablePlan{stmt: stmt}
}

func (d *AlterTablePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (at *AlterTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	if at.Shards == nil {
		// non-sharding alter table
		var sb strings.Builder
		if err := at.stmt.Restore(ast.RestoreDefault, &sb, nil); err != nil {
			return nil, err
		}
		return conn.Exec(ctx, "", sb.String(), at.args...)
	}
	var (
		affects = uatomic.NewUint64(0)
		cnt     = uatomic.NewUint32(0)
	)

	var g errgroup.Group

	// sharding alter table
	for k, v := range at.Shards {
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
				if err = at.stmt.ResetTable(table).Restore(ast.RestoreDefault, &sb, &args); err != nil {
					return errors.WithStack(err)
				}

				if res, err = conn.Exec(ctx, db, sb.String(), at.toArgs(args)...); err != nil {
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

	log.Debugf("sharding alter table success: batch=%d, affects=%d", cnt.Load(), affects.Load())

	return resultx.New(resultx.WithRowsAffected(affects.Load())), nil
}
