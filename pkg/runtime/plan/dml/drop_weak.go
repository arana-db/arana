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
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
)

var _ proto.Plan = (*DropWeakPlan)(nil)

// DropWeakPlan drops weak fields from upstream dataset.
//
// For example:
//      SELECT id,uid,name FROM student WHERE ... ORDER BY age DESC
// will be written:
//      SELECT id,uid,name,age FROM student WHERE ... ORDER BY age DESC
//
// the `age` field is weak, will be dropped finally.
type DropWeakPlan struct {
	proto.Plan
	WeakList []ast.SelectElement
}

func (dr DropWeakPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	res, err := dr.Plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	fields, err := ds.Fields()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		weakMap = dr.generateWeakMap()
		weakIdx = make(map[int]struct{}, len(weakMap))
	)
	for i := range fields {
		if _, ok := weakMap[fields[i].Name()]; ok {
			weakIdx[i] = struct{}{}
		}
	}
	actualFields := make([]proto.Field, 0, len(fields))
	for i := range fields {
		if _, ok := weakIdx[i]; ok {
			continue
		}
		actualFields = append(actualFields, fields[i])
	}

	newDs := dataset.Pipe(ds, dataset.Map(func(fields []proto.Field) []proto.Field {
		return actualFields
	}, func(row proto.Row) (proto.Row, error) {
		cells := make([]proto.Value, len(fields))
		if err := row.Scan(cells); err != nil {
			return nil, errors.WithStack(err)
		}

		var actualCells []proto.Value
		for i := range cells {
			if _, ok := weakIdx[i]; ok {
				continue
			}
			actualCells = append(actualCells, cells[i])
		}

		var ret proto.Row
		if row.IsBinary() {
			ret = rows.NewBinaryVirtualRow(actualFields, actualCells)
		} else {
			ret = rows.NewTextVirtualRow(actualFields, actualCells)
		}
		return ret, nil
	}))

	return resultx.New(resultx.WithDataset(newDs)), nil
}

func (dr DropWeakPlan) generateWeakMap() map[string]struct{} {
	var (
		weaks = make(map[string]struct{}, len(dr.WeakList))
		sb    strings.Builder
	)
	for _, it := range dr.WeakList {
		if alias := it.Alias(); len(alias) > 0 {
			weaks[alias] = struct{}{}
			continue
		}

		var root ast.SelectElement
		if p, ok := it.(ext.SelectElementProvider); ok {
			root = p.Prev()
		} else {
			root = it
		}

		switch val := root.(type) {
		case *ast.SelectElementColumn:
			weaks[val.Suffix()] = struct{}{}
		default:
			_ = val.Restore(ast.RestoreWithoutAlias, &sb, nil)
			weaks[sb.String()] = struct{}{}
			sb.Reset()
		}
	}

	return weaks
}
