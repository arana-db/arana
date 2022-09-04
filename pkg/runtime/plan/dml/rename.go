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
	"fmt"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
)

var _ proto.Plan = (*RenamePlan)(nil)

type RenamePlan struct {
	proto.Plan
	RenameList []string
}

func (rp RenamePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	res, err := rp.Plan.ExecIn(ctx, conn)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ds, err := res.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	convFields := func(fields []proto.Field) []proto.Field {
		if len(rp.RenameList) != len(fields) {
			panic(fmt.Sprintf("the length of field doesn't match: expect=%d, actual=%d!", len(rp.RenameList), len(fields)))
		}

		var renames map[int]struct{}
		for i := 0; i < len(rp.RenameList); i++ {
			rename := rp.RenameList[i]
			name := fields[i].Name()
			if rename == name {
				continue
			}
			if renames == nil {
				renames = make(map[int]struct{})
			}
			renames[i] = struct{}{}
		}

		if len(renames) < 1 {
			return fields
		}

		newFields := make([]proto.Field, 0, len(fields))
		for i := 0; i < len(fields); i++ {
			if _, ok := renames[i]; ok {
				f := *(fields[i].(*mysql.Field))
				f.SetName(rp.RenameList[i])
				f.SetOrgName(rp.RenameList[i])
				newFields = append(newFields, &f)
			} else {
				newFields = append(newFields, fields[i])
			}
		}
		return newFields
	}

	return resultx.New(resultx.WithDataset(dataset.Pipe(ds, dataset.Map(convFields, nil)))), nil
}
