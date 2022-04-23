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
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var _ proto.Plan = (*ShowTablesPlan)(nil)

type DatabaseTable struct {
	Database  string
	TableName string
}

type ShowTablesPlan struct {
	basePlan
	Database  string
	Stmt      *ast.ShowTables
	allShards map[string]DatabaseTable
}

// NewShowTablesPlan create ShowTables Plan
func NewShowTablesPlan(stmt *ast.ShowTables) *ShowTablesPlan {
	return &ShowTablesPlan{
		Stmt: stmt,
	}
}

func (s *ShowTablesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowTablesPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)

	if err = s.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		query = sb.String()
		args  = s.toArgs(indexes)
	)

	if res, err = conn.Query(ctx, s.Database, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}

	rebuildResult := mysql.Result{
		Fields: res.GetFields(),
	}

	hasRebuildTables := make(map[string]struct{})
	var affectRows uint64
	for _, row := range res.GetRows() {
		var innerRow mysql.Row
		switch r := row.(type) {
		case *mysql.BinaryRow:
			innerRow = r.Row
		case *mysql.Row:
			innerRow = *r
		case *mysql.TextRow:
			innerRow = r.Row
		}
		textRow := mysql.TextRow{Row: innerRow}
		rowValues, err := textRow.Decode()
		if err != nil {
			return nil, err
		}
		tableName := s.convertInterfaceToStrNullable(rowValues[0].Val)
		if databaseTable, exist := s.allShards[tableName]; exist {
			if _, ok := hasRebuildTables[databaseTable.TableName]; ok {
				continue
			}

			if _, ok := hasRebuildTables[databaseTable.TableName]; ok {
				continue
			}

			encodeTableName := mysql.PutLengthEncodedString([]byte(databaseTable.TableName))
			tmpValues := rowValues
			for idx := range tmpValues {
				tmpValues[idx].Val = string(encodeTableName)
				tmpValues[idx].Raw = encodeTableName
				tmpValues[idx].Len = len(encodeTableName)
			}

			var tmpNewRow mysql.Row
			tmpNewRow.Encode(tmpValues, textRow.Fields(), textRow.Columns())
			rebuildResult.Rows = append(rebuildResult.Rows, &tmpNewRow)
			hasRebuildTables[databaseTable.TableName] = struct{}{}
			affectRows++
			continue
		}
		affectRows++
		rebuildResult.Rows = append(rebuildResult.Rows, row)
	}
	rebuildResult.AffectedRows = affectRows
	return &rebuildResult, nil
}

func (s *ShowTablesPlan) convertInterfaceToStrNullable(value interface{}) string {
	if value != nil {
		return string(value.([]byte))
	}
	return ""
}

func (s *ShowTablesPlan) SetAllShards(allShards map[string]DatabaseTable) {
	s.allShards = allShards
}
