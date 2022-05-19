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

package optimize

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/parser"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/testdata"
)

func TestOptimizer_OptimizeSelect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)

	conn.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake query: db=%s, sql=%s, args=%v\n", db, sql, args)
			return &mysql.Result{}, nil
		}).
		AnyTimes()

	var (
		sql  = "select id, uid from student where uid in (?,?,?)"
		ctx  = context.Background()
		rule = makeFakeRule(ctrl, 8)
		opt  optimizer
	)

	p := parser.New()
	stmt, _ := p.ParseOneStmt(sql, "", "")

	plan, err := opt.Optimize(rcontext.WithRule(ctx, rule), conn, stmt, 1, 2, 3)
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

			return &mysql.Result{
				AffectedRows: uint64(strings.Count(sql, "?")),
				InsertId:     fakeId,
			}, nil
		}).
		AnyTimes()
	loader.EXPECT().Load(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(fakeStudentMetadata).Times(2)

	var (
		ctx  = context.Background()
		rule = makeFakeRule(ctrl, 8)
		opt  = optimizer{schemaLoader: loader}
	)

	t.Run("sharding", func(t *testing.T) {
		sql := "insert into student(name,uid,age) values('foo',?,18),('bar',?,19),('qux',?,17)"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		plan, err := opt.Optimize(rcontext.WithRule(ctx, rule), conn, stmt, 8, 9, 16) // 8,16 -> fake_db_0000, 9 -> fake_db_0001
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

		plan, err := opt.Optimize(rcontext.WithRule(ctx, rule), conn, stmt, 1)
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
	loader := testdata.NewMockSchemaLoader(ctrl)

	conn.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake exec: db='%s', sql=\"%s\", args=%v\n", db, sql, args)
			return &mysql.Result{}, nil
		}).AnyTimes()

	var (
		ctx  = context.Background()
		opt  = optimizer{schemaLoader: loader}
		ru   rule.Rule
		tab  rule.VTable
		topo rule.Topology
	)

	topo.SetRender(func(_ int) string {
		return "fake_db"
	}, func(i int) string {
		return fmt.Sprintf("student_%04d", i)
	})
	tables := make([]int, 0, 8)
	for i := 0; i < 8; i++ {
		tables = append(tables, i)
	}
	topo.SetTopology(0, tables...)
	tab.SetTopology(&topo)
	tab.SetAllowFullScan(true)
	ru.SetVTable("student", &tab)

	t.Run("sharding", func(t *testing.T) {
		sql := "alter table student add dept_id int not null default 0 after uid"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		plan, err := opt.Optimize(rcontext.WithRule(ctx, &ru), conn, stmt)
		assert.NoError(t, err)

		_, err = plan.ExecIn(ctx, conn)
		assert.NoError(t, err)

	})

	t.Run("non-sharding", func(t *testing.T) {
		sql := "alter table employees add index idx_name (first_name)"

		p := parser.New()
		stmt, _ := p.ParseOneStmt(sql, "", "")

		plan, err := opt.Optimize(rcontext.WithRule(ctx, &ru), conn, stmt)
		assert.NoError(t, err)

		_, err = plan.ExecIn(ctx, conn)
		assert.NoError(t, err)
	})
}
