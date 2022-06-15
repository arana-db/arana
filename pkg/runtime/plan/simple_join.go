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

type JoinTable struct {
	Tables []string
	Alias  string
}

type SimpleJoinPlan struct {
	basePlan
	Database string
	Left     *JoinTable
	Join     *ast.JoinNode
	Right    *JoinTable
	Stmt     *ast.SelectStatement
}

func (s *SimpleJoinPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *SimpleJoinPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := Tracer.Start(ctx, "SimpleJoinPlan.ExecIn")
	defer span.End()
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	if err := s.generateSelect(&sb, &indexes); err != nil {
		return nil, err
	}

	sb.WriteString(" FROM ")

	//add left part
	if err := s.generateTable(s.Left.Tables, s.Left.Alias, &sb); err != nil {
		return nil, err
	}

	s.generateJoinType(&sb)

	//add right part
	if err := s.generateTable(s.Right.Tables, s.Right.Alias, &sb); err != nil {
		return nil, err
	}

	// add on
	sb.WriteString(" ON ")

	if err := s.Join.On.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}
	if s.Stmt.Where != nil {
		sb.WriteString(" WHERE ")
		if err := s.Stmt.Where.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
			return nil, err
		}
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

func (s *SimpleJoinPlan) generateSelect(sb *strings.Builder, args *[]int) error {
	sb.WriteString("SELECT ")

	if s.Stmt.IsDistinct() {
		sb.WriteString(ast.Distinct)
		sb.WriteString(" ")
	}

	if err := s.Stmt.Select[0].Restore(ast.RestoreDefault, sb, args); err != nil {
		return errors.WithStack(err)
	}
	for i := 1; i < len(s.Stmt.Select); i++ {
		sb.WriteByte(',')
		if err := s.Stmt.Select[i].Restore(ast.RestoreDefault, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *SimpleJoinPlan) generateTable(tables []string, alias string, sb *strings.Builder) error {

	if len(tables) == 1 {
		sb.WriteString(tables[0] + " ")

		if alias != "" {
			sb.WriteString(" AS ")
			sb.WriteString(alias + " ")
		}
		return nil
	}

	sb.WriteString("( ")
	sb.WriteString("select * from " + tables[0] + " ")

	if len(tables) == 1 {
		sb.WriteString(" )")
		return nil
	}

	for i := 1; i < len(tables); i++ {
		sb.WriteString(" UNION ALL ")
		sb.WriteString("select * from " + tables[i] + " ")
	}
	sb.WriteString(" )")

	if alias != "" {
		sb.WriteString(" AS ")
		sb.WriteString(alias + " ")
	}
	return nil
}

func (s *SimpleJoinPlan) generateJoinType(sb *strings.Builder) {
	//add join type
	switch s.Join.Typ {
	case ast.LeftJoin:
		sb.WriteString("LEFT")
	case ast.RightJoin:
		sb.WriteString("RIGHT")
	case ast.InnerJoin:
		sb.WriteString("INNER")
	}
	sb.WriteString(" JOIN ")
}
