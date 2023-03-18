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

package dml

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

var _ proto.Plan = (*SimpleQueryPlan)(nil)

type SimpleQueryPlan struct {
	plan.BasePlan
	Database string
	Tables   []string
	Stmt     *ast.SelectStatement
}

func (s *SimpleQueryPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *SimpleQueryPlan) isCompat80Enabled(ctx context.Context, conn proto.VConn) bool {
	var (
		frontendVersion string
		backendVersion  string
	)

	if ver := rcontext.Version(ctx); len(ver) > 0 {
		frontendVersion = ver
	}
	if vs, ok := conn.(proto.VersionSupport); ok {
		backendVersion, _ = vs.Version(ctx)
	}

	if len(frontendVersion) < 1 || len(backendVersion) < 1 {
		return false
	}

	// 5.x -> 8.x+
	return backendVersion[0] >= '8' && frontendVersion[0] < '8'
}

func (s *SimpleQueryPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		rf      = ast.RestoreDefault
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	ctx, span := plan.Tracer.Start(ctx, "SimpleQueryPlan.ExecIn")
	defer span.End()

	discard := s.filter()

	if s.isCompat80Enabled(ctx, conn) {
		rf |= ast.RestoreCompat80
	}

	if err = s.generate(rf, &sb, &indexes); err != nil {
		return nil, errors.Wrap(err, "failed to generate sql")
	}

	var (
		query = sb.String()
		args  = s.ToArgs(indexes)
	)

	if res, err = conn.Query(ctx, s.Database, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}

	if !discard {
		return res, nil
	}

	var (
		rr     = res.(*mysql.RawResult)
		fields []proto.Field
	)

	defer func() {
		_ = rr.Discard()
	}()

	if fields, err = rr.Fields(); err != nil {
		return nil, errors.WithStack(err)
	}

	emptyDs := &dataset.VirtualDataset{
		Columns: fields,
	}
	return resultx.New(resultx.WithDataset(emptyDs)), nil
}

func (s *SimpleQueryPlan) filter() bool {
	if len(s.Stmt.From) <= 0 {
		return false
	}
	source, ok := s.Stmt.From[0].Source.(ast.TableName)
	if !ok {
		return false
	}
	if source.String() == "`information_schema`.`columns`" {
		return true
	}
	return false
}

func (s *SimpleQueryPlan) resetTable(tgt *ast.SelectStatement, table string) error {
	if len(tgt.From) != 1 {
		return errors.Errorf("cannot reset table because incorrect length of table: expect=1, actual=%d", len(tgt.From))
	}

	if ok := tgt.From[0].ResetTableName(table); !ok {
		return errors.New("cannot reset table name for select statement")
	}

	return nil
}

func (s *SimpleQueryPlan) generate(rf ast.RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch len(s.Tables) {
	case 0:
		// no table reset
		if err := s.Stmt.Restore(rf, sb, args); err != nil {
			return errors.WithStack(err)
		}
	case 1:
		// single shard table
		var (
			stmt = *s.Stmt
			err  error
		)
		if err = s.resetTable(&stmt, s.Tables[0]); err != nil {
			return errors.WithStack(err)
		}
		if err = stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
			return errors.WithStack(err)
		}
	default:
		// multiple shard tables: zip by UNION_ALL
		//
		// Image that there's a logical table with a rule of 'school.student_{0000..0008}'.
		// For a simple query:
		//     SELECT * FROM student WHERE uid IN (1,2,3)
		// That can be converted to a single sql:
		//     (SELECT * FROM student_0001 WHERE uid IN (1,2,3))
		//        UNION ALL
		//     (SELECT * FROM student_0002 WHERE uid IN (1,2,3))
		//        UNION ALL
		//     (SELECT * FROM student_0000 WHERE uid IN (1,2,3)

		stmt := new(ast.SelectStatement)
		*stmt = *s.Stmt // do copy

		restore := func(table string) error {
			sb.WriteByte('(')
			if err := s.resetTable(stmt, table); err != nil {
				return err
			}
			if err := stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
				return err
			}
			sb.WriteByte(')')
			stmt.From = s.Stmt.From
			return nil
		}

		if err := restore(s.Tables[0]); err != nil {
			return errors.WithStack(err)
		}

		for i := 1; i < len(s.Tables); i++ {
			sb.WriteString(" UNION ALL ")

			if err := restore(s.Tables[i]); err != nil {
				return errors.WithStack(err)
			}
		}

		if length := len(stmt.OrderBy); length > 0 {
			sb.WriteString(" ORDER BY ")
			if err := stmt.OrderBy[0].Restore(ast.RestoreDefault, sb, args); err != nil {
				return errors.WithStack(err)
			}

			for i := 1; i < length; i++ {
				sb.WriteString(", ")
				if err := stmt.OrderBy[i].Restore(ast.RestoreDefault, sb, args); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}

	return nil
}
