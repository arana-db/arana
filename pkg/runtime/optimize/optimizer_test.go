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

package optimize_test

import (
	"context"
	"fmt"
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"strings"
	"testing"
)

import (
	"github.com/arana-db/parser"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/resultx"
	. "github.com/arana-db/arana/pkg/runtime/optimize"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/dal"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/ddl"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/dml"
	_ "github.com/arana-db/arana/pkg/runtime/optimize/utility"
	"github.com/arana-db/arana/testdata"
)

func TestOptimizer_OptimizeSelect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)

	conn.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake query: db=%s, sql=%s, args=%v\n", db, sql, args)

			ds := testdata.NewMockDataset(ctrl)
			ds.EXPECT().Fields().Return([]proto.Field{}, nil).AnyTimes()

			return resultx.New(resultx.WithDataset(ds)), nil
		}).
		AnyTimes()

	var (
		sql = "select id, uid from student where uid in (?,?,?)"
		ctx = context.WithValue(context.Background(), proto.ContextKeyEnableLocalComputation{}, true)
		ru  = makeFakeRule(ctrl, "student", 8, nil)
	)

	p := parser.New()
	stmt, _ := p.ParseOneStmt(sql, "", "")
	opt, err := NewOptimizer(ru, nil, stmt, []proto.Value{
		proto.NewValueInt64(1),
		proto.NewValueInt64(2),
		proto.NewValueInt64(3),
	})
	assert.NoError(t, err)
	plan, err := opt.Optimize(ctx)
	assert.NoError(t, err)

	_, _ = plan.ExecIn(ctx, conn)
}

func TestOptimizer_OptimizeHashJoin(t *testing.T) {
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

	conn := testdata.NewMockVConn(ctrl)
	buildPlan := true
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
		sql = "select * from student join salaries on uid = emp_no"
		ctx = context.WithValue(context.Background(), proto.ContextKeyEnableLocalComputation{}, true)
		ru  = makeFakeRule(ctrl, "student", 8, nil)
	)

	ru = makeFakeRule(ctrl, "salaries", 8, ru)

	p := parser.New()
	stmt, _ := p.ParseOneStmt(sql, "", "")
	opt, err := NewOptimizer(ru, nil, stmt, nil)
	assert.NoError(t, err)

	vTable, _ := ru.VTable("student")
	vTable.SetAllowFullScan(true)

	vTable2, _ := ru.VTable("salaries")
	vTable2.SetAllowFullScan(true)

	plan, err := opt.Optimize(ctx)
	assert.NoError(t, err)

	_, _ = plan.ExecIn(ctx, conn)
}

func TestOptimizer_OptimizeInsert(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)
	loader := testdata.NewMockSchemaLoader(ctrl)

	var fakeId uint64

	fakeStudentMetadata := make(map[string]*proto.TableMetadata)
	fakeStuColumnMetadata := make(map[string]*proto.ColumnMetadata)
	fakeStuColumnMetadata["name"] = &proto.ColumnMetadata{
		Name:          "name",
		DataType:      "varchar",
		Ordinal:       "1",
		PrimaryKey:    false,
		Generated:     false,
		CaseSensitive: false,
	}
	fakeStuColumnMetadata["uid"] = &proto.ColumnMetadata{
		Name:          "uid",
		DataType:      "bigint",
		Ordinal:       "2",
		PrimaryKey:    false,
		Generated:     false,
		CaseSensitive: false,
	}
	fakeStuColumnMetadata["age"] = &proto.ColumnMetadata{
		Name:          "age",
		DataType:      "tinyint",
		Ordinal:       "3",
		PrimaryKey:    false,
		Generated:     false,
		CaseSensitive: false,
	}
	fakeStudentMetadata["student"] = &proto.TableMetadata{
		Name:              "student",
		Columns:           fakeStuColumnMetadata,
		Indexes:           nil,
		ColumnNames:       []string{"name", "uid", "age"},
		PrimaryKeyColumns: nil,
	}

	conn.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake exec: db='%s', sql=\"%s\", args=%v\n", db, sql, args)
			fakeId++

			return resultx.New(
				resultx.WithRowsAffected(uint64(strings.Count(sql, "?"))),
				resultx.WithLastInsertID(fakeId),
			), nil
		}).
		AnyTimes()
	loader.EXPECT().Load(gomock.Any(), gomock.Any(), gomock.Any()).Return(fakeStudentMetadata, nil).Times(2)

	oldLoader := proto.LoadSchemaLoader()
	proto.RegisterSchemaLoader(loader)
	defer proto.RegisterSchemaLoader(oldLoader)

	var (
		ctx = context.Background()
		ru  = makeFakeRule(ctrl, "student", 8, nil)
	)

	t.Run("sharding", func(t *testing.T) {
		sql := "insert into student(name,uid,age) values('foo',?,18),('bar',?,19),('qux',?,17)"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		opt, err := NewOptimizer(ru, nil, stmt, []proto.Value{
			proto.NewValueInt64(8),
			proto.NewValueInt64(9),
			proto.NewValueInt64(16),
		})
		assert.NoError(t, err)

		plan, err := opt.Optimize(ctx) // 8,16 -> fake_db_0000, 9 -> fake_db_0001
		assert.NoError(t, err)

		res, err := plan.ExecIn(ctx, conn)
		assert.NoError(t, err)

		affected, _ := res.RowsAffected()
		assert.Equal(t, uint64(3), affected)
		lastInsertId, _ := res.LastInsertId()
		assert.Equal(t, fakeId, lastInsertId)
	})

	t.Run("non-sharding", func(t *testing.T) {
		sql := "insert into abc set name='foo',uid=?,age=18"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		opt, err := NewOptimizer(ru, nil, stmt, []proto.Value{proto.NewValueInt64(1)})
		assert.NoError(t, err)

		plan, err := opt.Optimize(ctx)
		assert.NoError(t, err)

		res, err := plan.ExecIn(ctx, conn)
		assert.NoError(t, err)

		affected, _ := res.RowsAffected()
		assert.Equal(t, uint64(1), affected)
		lastInsertId, _ := res.LastInsertId()
		assert.Equal(t, fakeId, lastInsertId)
	})
}

func TestOptimizer_OptimizeAlterTable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)

	conn.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake exec: db='%s', sql=\"%s\", args=%v\n", db, sql, args)
			return resultx.New(), nil
		}).AnyTimes()

	var (
		ctx      = context.Background()
		ru       rule.Rule
		tab      rule.VTable
		topology rule.Topology
	)

	topology.SetRender(func(_ int) string {
		return "fake_db"
	}, func(i int) string {
		return fmt.Sprintf("student_%04d", i)
	})
	tables := make([]int, 0, 8)
	for i := 0; i < 8; i++ {
		tables = append(tables, i)
	}
	topology.SetTopology(0, tables...)
	tab.SetTopology(&topology)
	tab.SetAllowFullScan(true)
	ru.SetVTable("student", &tab)

	t.Run("sharding", func(t *testing.T) {
		sql := "alter table student add dept_id int not null default 0 after uid"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		opt, err := NewOptimizer(&ru, nil, stmt, nil)
		assert.NoError(t, err)

		plan, err := opt.Optimize(ctx)
		assert.NoError(t, err)

		_, err = plan.ExecIn(ctx, conn)
		assert.NoError(t, err)
	})

	t.Run("non-sharding", func(t *testing.T) {
		sql := "alter table employees add index idx_name (first_name)"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		opt, err := NewOptimizer(&ru, nil, stmt, nil)
		assert.NoError(t, err)

		plan, err := opt.Optimize(ctx)
		assert.NoError(t, err)

		_, err = plan.ExecIn(ctx, conn)
		assert.NoError(t, err)
	})
}

func TestOptimizer_OptimizeInsertSelect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)

	var fakeId uint64
	conn.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake exec: db='%s', sql=\"%s\", args=%v\n", db, sql, args)
			fakeId++

			return resultx.New(
				resultx.WithRowsAffected(uint64(strings.Count(sql, "?"))),
				resultx.WithLastInsertID(fakeId),
			), nil
		}).
		AnyTimes()

	var (
		ctx = context.Background()
		ru  rule.Rule
	)

	ru.SetVTable("student", nil)

	t.Run("non-sharding", func(t *testing.T) {
		sql := "insert into employees(name, age) select name,age from employees_tmp limit 10,2"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		opt, err := NewOptimizer(&ru, nil, stmt, []proto.Value{proto.NewValueInt64(1)})
		assert.NoError(t, err)

		plan, err := opt.Optimize(ctx)
		assert.NoError(t, err)

		res, err := plan.ExecIn(ctx, conn)
		assert.NoError(t, err)

		affected, _ := res.RowsAffected()
		assert.Equal(t, uint64(0), affected)
		lastInsertId, _ := res.LastInsertId()
		assert.Equal(t, fakeId, lastInsertId)
	})
}
