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
	"io"
	"testing"
)

import (
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/testdata"
)

func TestHashJoinPlan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	studentFields := []proto.Field{
		mysql.NewField("uid", consts.FieldTypeLongLong),
		mysql.NewField("name", consts.FieldTypeString),
	}

	salariesFields := []proto.Field{
		mysql.NewField("emp_no", consts.FieldTypeLongLong),
		mysql.NewField("name", consts.FieldTypeString),
	}

	buildPlan := true
	conn := testdata.NewMockVConn(ctrl)
	conn.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake query: db=%s, sql=%s, args=%v\n", db, sql, args)

			result := testdata.NewMockResult(ctrl)
			fakeData := &dataset.VirtualDataset{}
			if buildPlan {
				fakeData.Columns = append(studentFields, mysql.NewField("uid", consts.FieldTypeLongLong))
				for i := int64(0); i < 8; i++ {
					fakeData.Rows = append(fakeData.Rows, rows.NewTextVirtualRow(fakeData.Columns, []proto.Value{
						proto.NewValueInt64(i),
						proto.NewValueString(fmt.Sprintf("fake-student-name-%d", i)),
						proto.NewValueInt64(i),
					}))
				}
				result.EXPECT().Dataset().Return(fakeData, nil).AnyTimes()
				buildPlan = false
			} else {
				fakeData.Columns = append(salariesFields, mysql.NewField("emp_no", consts.FieldTypeLongLong))
				for i := int64(10); i > 3; i-- {
					fakeData.Rows = append(fakeData.Rows, rows.NewTextVirtualRow(fakeData.Columns, []proto.Value{
						proto.NewValueInt64(i),
						proto.NewValueString(fmt.Sprintf("fake-salaries-name-%d", i)),
						proto.NewValueInt64(i),
					}))
				}
				result.EXPECT().Dataset().Return(fakeData, nil).AnyTimes()
			}

			return result, nil
		}).
		AnyTimes()

	var (
		sql1 = "SELECT * FROM student"  // mock build plan
		sql2 = "SELECT * FROM salaries" // mock probe plan
		ctx  = context.WithValue(context.Background(), proto.ContextKeyEnableLocalComputation{}, true)
	)

	_, stmt1, _ := ast.ParseSelect(sql1)
	_, stmt2, _ := ast.ParseSelect(sql2)

	// sql: select * from student join salaries on uid = emp_no;
	plan := &HashJoinPlan{
		BuildPlan: CompositePlan{
			[]proto.Plan{
				&SimpleQueryPlan{
					Stmt: stmt1,
				},
			},
		},
		ProbePlan: CompositePlan{
			[]proto.Plan{
				&SimpleQueryPlan{
					Stmt: stmt2,
				},
			},
		},
		IsFilterProbeRow: true,
		BuildKey:         "uid",
		ProbeKey:         "emp_no",
	}

	res, err := plan.ExecIn(ctx, conn)
	assert.NoError(t, err)
	ds, _ := res.Dataset()
	f, _ := ds.Fields()

	// expected field
	assert.Equal(t, "uid", f[0].Name())
	assert.Equal(t, "name", f[1].Name())
	assert.Equal(t, "emp_no", f[2].Name())
	assert.Equal(t, "name", f[3].Name())
	for {
		next, err := ds.Next()
		if err == io.EOF {
			break
		}
		row := next.(proto.Row)
		dest := make([]proto.Value, len(f))
		_ = row.Scan(dest)

		// expected value:  uid = emp_no
		assert.Equal(t, dest[0], dest[2])
	}

}
