// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

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
	Database string
	Tables   []string
	Stmt     *ast.SelectStatement
	Args     []interface{}
}

func (s *SimpleQueryPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *SimpleQueryPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb         strings.Builder
		argIndexes []int
		res        proto.Result
		err        error
	)

	if err = s.generate(&sb, &argIndexes); err != nil {
		return nil, errors.Wrap(err, "failed to generate sql")
	}

	if len(argIndexes) < 1 {
		res, err = conn.Query(ctx, s.Database, sb.String())
	} else {
		args := make([]interface{}, 0, len(argIndexes))
		for _, idx := range argIndexes {
			args = append(args, s.Args[idx])
		}
		res, err = conn.Query(ctx, s.Database, sb.String(), args...)
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return res, nil
}

func (s *SimpleQueryPlan) generate(sb *strings.Builder, args *[]int) (err error) {
	switch len(s.Tables) {
	case 0:
		err = generateSelect("", s.Stmt, sb, args)
	case 1:
		// single shard table
		err = generateSelect(s.Tables[0], s.Stmt, sb, args)
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
		sb.WriteByte('(')
		if err = generateSelect(s.Tables[0], s.Stmt, sb, args); err != nil {
			return
		}
		sb.WriteByte(')')

		for i := 1; i < len(s.Tables); i++ {
			sb.WriteString(" UNION ALL ")

			sb.WriteByte('(')
			if err = generateSelect(s.Tables[i], s.Stmt, sb, args); err != nil {
				return
			}
			sb.WriteByte(')')
		}
	}
	return
}
