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

var _ proto.Plan = (*SimpleQueryPlan)(nil)

type SimpleQueryPlan struct {
	basePlan
	Database string
	Tables   []string
	Stmt     *ast.SelectStatement
}

func (s *SimpleQueryPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *SimpleQueryPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	if err = s.generate(&sb, &indexes); err != nil {
		return nil, errors.Wrap(err, "failed to generate sql")
	}

	var (
		query = sb.String()
		args  = s.toArgs(indexes)
	)

	if res, err = conn.Query(ctx, s.Database, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
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

func (s *SimpleQueryPlan) generate(sb *strings.Builder, args *[]int) error {
	switch len(s.Tables) {
	case 0:
		// no table reset
		if err := s.Stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
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

		var (
			stmt = new(ast.SelectStatement)
		)
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
	}

	return nil
}
