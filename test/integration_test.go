//go:build integration
// +build integration

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

package test

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

import (
	"github.com/arana-db/parser"

	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

import (
	"github.com/arana-db/arana/pkg/util/rand2"
	utils "github.com/arana-db/arana/pkg/util/tableprint"
)

type IntegrationSuite struct {
	*MySuite
}

func TestSuite(t *testing.T) {
	su := NewMySuite(
		WithMySQLServerAuth("root", "123456"),
		WithMySQLDatabase("employees"),
		WithConfig("../integration_test/config/db_tbl/config.yaml"),
		WithScriptPath("../scripts"),
		// WithDevMode(), // NOTICE: UNCOMMENT IF YOU WANT TO DEBUG LOCAL ARANA SERVER!!!
	)
	suite.Run(t, &IntegrationSuite{su})
}

func (s *IntegrationSuite) TestBasicTx() {
	var (
		db = s.DB()
		t  = s.T()
	)
	tx, err := db.Begin()
	assert.NoError(t, err, "should begin a new tx")

	var (
		name  = fmt.Sprintf("fake_name_%d", time.Now().UnixNano())
		value = rand2.Int31n(1000)
	)

	res, err := tx.Exec("INSERT INTO sequence(name,value,modified_at) VALUES(?,?,NOW())", name, value)
	assert.NoError(t, err, "should insert ok")
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	row := tx.QueryRow("SELECT COUNT(1) FROM sequence WHERE name=?", name)
	assert.NoError(t, row.Err())
	var cnt int
	err = row.Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)

	err = tx.Rollback()
	assert.NoError(t, err, "should rollback ok")

	row = db.QueryRow("SELECT COUNT(1) FROM sequence WHERE name=?", name)
	assert.NoError(t, row.Err())
	err = row.Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 0, cnt)

	// test commit
	tx, err = db.Begin()
	assert.NoError(t, err)

	res, err = tx.Exec("INSERT INTO sequence(name,value,modified_at) VALUES(?,?,NOW())", name, value)
	assert.NoError(t, err, "should insert ok")
	affected, err = res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	err = tx.Commit()
	assert.NoError(t, err, "should commit ok")

	row = db.QueryRow("SELECT COUNT(1) FROM sequence WHERE name=?", name)
	assert.NoError(t, row.Err())
	err = row.Scan(&cnt)
	assert.NoError(t, err)
	assert.Equal(t, 1, cnt)

	_, _ = db.Exec("delete from sequence where name = ?", name)
}

func (s *IntegrationSuite) TestSimpleSharding() {
	var (
		db = s.DB()
		t  = s.T()
	)

	const total = 100

	// insert into logical table
	for i := 1; i <= total; i++ {
		result, err := db.Exec(
			`INSERT IGNORE INTO student(id,uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?,?)`,
			time.Now().UnixNano(),
			i,
			3.14,
			fmt.Sprintf("fake_name_%d", i),
			fmt.Sprintf("fake_nickname_%d", i),
			1,
			2022-rand2.Intn(40),
		)
		assert.NoErrorf(t, err, "insert row error: %v", err)
		affected, err := result.RowsAffected()
		assert.NoErrorf(t, err, "insert row error: %v", err)
		assert.True(t, affected <= 1)
	}

	type tt struct {
		sql       string
		args      []interface{}
		expectLen int
	}

	for _, it := range []tt{
		{"SELECT * FROM student WHERE uid = 42 AND 1=2", nil, 0},
		{"SELECT * FROM student WHERE uid = ?", []interface{}{42}, 1},
		{"SELECT * FROM student WHERE uid in (?,?,?)", []interface{}{1, 2, 33}, 3},
		{"SELECT * FROM student where uid between 1 and 10", nil, 10},
		//{"SELECT * FROM student", nil, total}, // TODO: fix -> packet got EOF
	} {
		t.Run(it.sql, func(t *testing.T) {
			// select from logical table
			rows, err := db.Query(it.sql, it.args...)
			assert.NoError(t, err, "should query from sharding table successfully")
			defer rows.Close()
			data, _ := utils.PrintTable(rows)
			assert.Equal(t, it.expectLen, len(data))
		})
	}

	const wKey = 44
	t.Run("Update", func(t *testing.T) {
		res, err := db.Exec("update student set score=100.0 where uid = ?", wKey)
		assert.NoError(t, err)
		affected, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), affected)

		// validate
		row := db.QueryRow("select score from student where uid = ?", wKey)
		assert.NoError(t, row.Err())
		var score float32
		assert.NoError(t, row.Scan(&score))
		assert.Equal(t, float32(100), score)
	})

	t.Run("Delete", func(t *testing.T) {
		res, err := db.Exec("delete from student where uid = 13 OR uid = ?", wKey)
		assert.NoError(t, err)
		affected, err := res.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), affected)
	})
}

func (s *IntegrationSuite) TestInsert() {
	var (
		db = s.DB()
		t  = s.T()
	)
	result, err := db.Exec(`INSERT INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date )
		VALUES (?, ?, ?, ?, ?, ?)  `, 100001, "1992-01-07", "scott", "lewis", "M", "2014-09-01")
	assert.NoErrorf(t, err, "insert row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)
	assert.Equal(t, int64(1), affected)
}

func (s *IntegrationSuite) TestInsertOnDuplicateKey() {
	var (
		db = s.DB()
		t  = s.T()
	)

	i := 32
	result, err := db.Exec(`INSERT IGNORE INTO student(id,uid,score,name,nickname,gender,birth_year)
     values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE nickname='dump' `, 1654008174496657000, i, 3.14, fmt.Sprintf("fake_name_%d", i), fmt.Sprintf("fake_nickname_%d", i), 1, 2022-rand2.Intn(40))
	assert.NoErrorf(t, err, "insert row error: %v", err)
	_, err = result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)

	_, err = db.Exec(`INSERT IGNORE INTO student(id,uid,score,name,nickname,gender,birth_year)
     values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE uid=32 `, 1654008174496657000, i, 3.14, fmt.Sprintf("fake_name_%d", i), fmt.Sprintf("fake_nickname_%d", i), 1, 2022-rand2.Intn(40))
	assert.Error(t, err, "insert row error: %v", err)
}

func (s *IntegrationSuite) TestSelect() {
	var (
		db = s.DB()
		t  = s.T()
	)

	rows, err := db.Query(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees
		WHERE emp_no = ?`, "100001")
	assert.NoErrorf(t, err, "select row error: %v", err)

	defer rows.Close()

	var empNo string
	var birthDate string
	var firstName string
	var lastName string
	var gender string
	var hireDate string
	if rows.Next() {
		err = rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", firstName)
}

func (s *IntegrationSuite) TestSelectLimit1() {
	var (
		db = s.DB()
		t  = s.T()
	)

	rows, err := db.Query(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees LIMIT 1`)
	assert.NoErrorf(t, err, "select row error: %v", err)

	defer rows.Close()

	var empNo string
	var birthDate string
	var firstName string
	var lastName string
	var gender string
	var hireDate string
	if rows.Next() {
		err = rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", firstName)
}

func (s *IntegrationSuite) TestUpdate() {
	var (
		db = s.DB()
		t  = s.T()
	)

	result, err := db.Exec(`UPDATE employees set last_name = ? where emp_no = ?`, "louis", 100001)
	assert.NoErrorf(t, err, "update row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "update row error: %v", err)

	assert.Equal(t, int64(1), affected)

	_, err = db.Exec("update student set score=100.0,uid=11 where uid = ?", 32)
	assert.Error(t, err)
}

func (s *IntegrationSuite) TestDelete() {
	var (
		db = s.DB()
		t  = s.T()
	)

	// prepare test records
	_, _ = db.Exec(`INSERT IGNORE INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date )
		VALUES (?, ?, ?, ?, ?, ?)`, 100001, "1992-01-07", "scott", "lewis", "M", "2014-09-01")

	result, err := db.Exec(`DELETE FROM employees WHERE emp_no = ?`, 100001)
	assert.NoErrorf(t, err, "delete row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "delete row error: %v", err)
	assert.Equal(t, int64(1), affected)
}

func (s *IntegrationSuite) TestShowDatabases() {
	var (
		db = s.DB()
		t  = s.T()
	)

	result, err := db.Query("show databases")
	assert.NoErrorf(t, err, "show databases error: %v", err)

	defer result.Close()

	affected, err := result.ColumnTypes()
	assert.NoErrorf(t, err, "show databases: %v", err)
	assert.Equal(t, affected[0].DatabaseTypeName(), "VARCHAR")
}

func (s *IntegrationSuite) TestDropTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	t.Skip()

	// drop table  physical name != logical name  and  physical name = logical name
	result, err := db.Exec(`DROP TABLE student,salaries`)

	assert.NoErrorf(t, err, "drop table error:%v", err)
	affected, err := result.RowsAffected()
	assert.Equal(t, int64(0), affected)
	assert.NoErrorf(t, err, "drop table  error: %v", err)

	// drop again, return error
	result, err = db.Exec(`DROP TABLE student,salaries`)
	assert.Error(t, err, "drop table error: %v", err)
	assert.Nil(t, result)
}

func (s *IntegrationSuite) TestJoinTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	t.Skip()

	sqls := []string{
		// shard  & no shard
		`select * from student  join titles on student.id=titles.emp_no`,
		// shard  & no shard with alias
		`select * from student  join titles as b on student.id=b.emp_no`,
		// shard  with alias & no shard with alias
		`select * from student as a join titles as b on a.id=b.emp_no`,
		// no shard & no shard
		`select * from departments join dept_emp as de on departments.dept_no = de.dept_no`,
	}

	for _, sql := range sqls {
		_, err := db.Query(sql)
		assert.NoErrorf(t, err, "join table error:%v", err)
	}
	// with where
	_, err := db.Query(`select * from student  join titles on student.id=titles.emp_no where student.id=? and titles.emp_no=?`, 1, 2)
	assert.NoErrorf(t, err, "join table error:%v", err)
}

func (s *IntegrationSuite) TestShardingAgg() {
	var (
		db = s.DB()
		t  = s.T()
	)

	if _, err := db.Exec("DELETE FROM student"); err != nil {
		t.Fatal(err)
	}

	const total = 100

	// insert into logical table
	for i := 1; i <= total; i++ {

		var (
			result sql.Result
			err    error
		)

		if i == 1 {
			result, err = db.Exec(
				`INSERT IGNORE INTO student(uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?)`,
				i,
				95,
				fmt.Sprintf("fake_name_%d", i),
				fmt.Sprintf("fake_nickname_%d", i),
				1,
				2022-rand2.Intn(40),
			)
		} else {
			result, err = db.Exec(
				`INSERT IGNORE INTO student(uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?)`,
				i,
				10,
				fmt.Sprintf("fake_name_%d", i),
				fmt.Sprintf("fake_nickname_%d", i),
				1,
				2022-rand2.Intn(40),
			)
		}
		assert.NoErrorf(t, err, "insert row error: %v", err)
		affected, err := result.RowsAffected()
		assert.NoErrorf(t, err, "insert row error: %v", err)
		assert.True(t, affected <= 1)
	}

	t.Run("Count", func(t *testing.T) {
		row := db.QueryRow("select count(*) as ttt from student")
		var cnt int64
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, int64(100), cnt)

		row = db.QueryRow("select count(*) div 3 as ttt from student")
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, int64(33), cnt)
	})

	t.Run("MAX", func(t *testing.T) {
		row := db.QueryRow("select max(score) as ttt from student")
		var cnt float64
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, 95, int(cnt))
	})

	t.Run("MIN", func(t *testing.T) {
		row := db.QueryRow("select min(score) as ttt from student")
		var cnt float64
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, float64(10), cnt)
	})

	t.Run("SUM", func(t *testing.T) {
		row := db.QueryRow("select sum(score) as ttt from student")
		var cnt float64
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, int64(1085), int64(cnt))
	})

	t.Run("AVG", func(t *testing.T) {
		row := db.QueryRow("select avg(score) as ttt from student")
		var avg float64
		assert.NoError(t, row.Scan(&avg))
		assert.True(t, avg > 0)
	})

	result, err := db.Exec(
		`INSERT IGNORE INTO student(uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?)`,
		9527,
		100,
		"jason",
		"jason",
		1,
		2022-rand2.Intn(40),
	)
	assert.NoErrorf(t, err, "insert row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)
	assert.True(t, affected <= 1)
	result, err = db.Exec(
		`INSERT IGNORE INTO student(id,uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?,?)`,
		time.Now().UnixNano(),
		9559,
		100,
		"jason",
		"jason",
		1,
		2022-rand2.Intn(40),
	)
	assert.NoErrorf(t, err, "insert row error: %v", err)
	affected, err = result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)
	assert.True(t, affected <= 1)

	type tt struct {
		sql       string
		args      []interface{}
		expectLen int
	}

	for _, it := range []tt{
		{"SELECT * FROM student WHERE uid >= 9527", nil, 2},
	} {
		t.Run(it.sql, func(t *testing.T) {
			// select from logical table
			rows, err := db.Query(it.sql, it.args...)
			assert.NoError(t, err, "should query from sharding table successfully")
			defer rows.Close()
			data, _ := utils.PrintTable(rows)
			assert.Equal(t, it.expectLen, len(data))
		})
	}

	t.Run("SUM_Jason", func(t *testing.T) {
		row := db.QueryRow("select sum(score) as ttt from student where uid >= 9527")
		var cnt float64
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, 200, int(cnt))
	})

	t.Run("COUNT_Jason", func(t *testing.T) {
		row := db.QueryRow("select count(score) as ttt from student where uid >= 9527")
		var cnt int
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, 2, cnt)
	})

	db.Exec("DELETE FROM student WHERE uid >= 9527")
}

func (s *IntegrationSuite) TestAlterTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	result, err := db.Exec(`alter table employees add dept_no char(4) not null default "" after emp_no`)
	assert.NoErrorf(t, err, "alter table error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "alter table error: %v", err)

	assert.Equal(t, int64(0), affected)
}

func (s *IntegrationSuite) TestCreateIndex() {
	var (
		db = s.DB()
		t  = s.T()
	)

	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "create index normally",
			sql:  "create index `name` on student (name)",
		},
		{
			name: "create index with index option",
			sql:  "create index `name` on student (name) USING BTREE COMMENT 'TEST COMMENT' ALGORITHM DEFAULT LOCK DEFAULT",
		},
	}

	for _, it := range tests {
		t.Run(it.name, func(t *testing.T) {
			result, err := db.Exec(it.sql)
			assert.NoError(t, err)
			affected, err := result.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, int64(0), affected)

			_, err = db.Exec("drop index name on student")
			assert.NoError(t, err)
		})
	}
}

func (s *IntegrationSuite) TestDropIndex() {
	var (
		db = s.DB()
		t  = s.T()
	)

	result, err := db.Exec("drop index `nickname` on student")
	assert.NoErrorf(t, err, "drop index error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "drop index error: %v", err)

	assert.Equal(t, int64(0), affected)
}

func (s *IntegrationSuite) TestShowColumns() {
	var (
		db = s.DB()
		t  = s.T()
	)

	result, err := db.Query("show columns from student")
	assert.NoErrorf(t, err, "show columns error: %v", err)

	defer result.Close()

	affected, err := result.ColumnTypes()
	assert.NoErrorf(t, err, "show columns: %v", err)
	assert.Equal(t, affected[0].DatabaseTypeName(), "VARCHAR")
}

func (s *IntegrationSuite) TestShowCreate() {
	var (
		db = s.DB()
		t  = s.T()
	)

	row := db.QueryRow("show create table student")
	var table, createStr string
	assert.NoError(t, row.Scan(&table, &createStr))
	assert.Equal(t, "student", table)
}

func (s *IntegrationSuite) TestDropTrigger() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range []tt{
		{"DROP TRIGGER arana"},
		{"DROP TRIGGER employees_0000.arana"},
		{"DROP TRIGGER IF EXISTS arana"},
		{"DROP TRIGGER IF EXISTS employees_0000.arana"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			_, err := db.Exec(it.sql)
			assert.NoError(t, err)
		})
	}
}

func (s *IntegrationSuite) TestOrderBy() {
	var (
		db   = s.DB()
		t    = s.T()
		uids []int
		base = int(time.Now().UnixMilli())
	)
	for i := 1; i <= 10; i++ {
		var (
			id  = 101*base + i
			uid = 10*base + i
		)
		uids = append(uids, uid)
		result, err := db.Exec(
			`INSERT INTO student(id,uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?,?)`,
			id,
			uid,
			float64(rand2.Intn(100))+0.05,
			fmt.Sprintf("fake_name_%d", uid),
			fmt.Sprintf("fake_nickname_%d", uid),
			1,
			1980+i,
		)
		assert.NoErrorf(t, err, "insert row error: %v", err)
		affected, err := result.RowsAffected()
		assert.NoErrorf(t, err, "insert row error: %v", err)
		assert.True(t, affected <= 1)
	}

	// cleanup
	defer func() {
		var deleted []interface{}
		for i := range uids {
			deleted = append(deleted, uids[i])
		}
		_, _ = db.Exec(
			fmt.Sprintf(
				"delete from student where uid in (%s)",
				strings.Repeat("?,", len(deleted)-1)+"?",
			),
			deleted...,
		)
	}()

	type tt struct {
		columns []string
		sql     string
		desc    bool
	}

	for _, it := range []tt{
		{[]string{"uid", "name", "birth_year"}, "select uid,name,birth_year from student where uid between ? and ? order by birth_year", false},
		{[]string{"uid", "name"}, "select uid,name from student where uid between ? and ? order by birth_year", false},
		{[]string{"uid", "name"}, "select uid,name from student where uid between ? and ? order by birth_year desc", true},
		{[]string{"uid", "name", "birth"}, "select uid,name,birth_year as birth from student where uid between ? and ? order by birth_year desc", true},
		{[]string{"uid", "name", "birth"}, "select uid,name,birth_year as birth from student where uid between ? and ? order by -birth_year", true},
		{[]string{"uid", "name", "birth"}, "select uid,name,birth_year as birth from student where uid between ? and ? order by 2022-birth_year", true},
		{[]string{"uid", "name", "age"}, "select uid,name,2022-birth_year as age from student where uid between ? and ? order by 2022-birth_year", true},
		{[]string{"uid", "name"}, "select uid,name from student where uid between ? and ? order by 2022-birth_year", true},
		{[]string{"uid", "name", "2022-birth_year"}, "select uid,name,2022-birth_year from student where uid between ? and ? order by 2022-birth_year", true},
	} {
		s.T().Run(it.sql, func(t *testing.T) {
			begin := uids[0]
			end := uids[len(uids)-1]
			rows, err := db.Query(it.sql, begin, end)
			assert.NoError(t, err)
			defer rows.Close()

			columns, _ := rows.Columns()
			assert.Equal(t, it.columns, columns)

			records, _ := utils.PrintTable(rows)
			assert.Len(t, records, len(uids))

			var actualUids []int
			for _, record := range records {
				uid, _ := strconv.Atoi(record[0])
				actualUids = append(actualUids, uid)
			}
			if it.desc {
				for i, j := 0, len(actualUids)-1; i < j; i, j = i+1, j-1 {
					actualUids[i], actualUids[j] = actualUids[j], actualUids[i]
				}
			}
			assert.True(t, sort.IsSorted(sort.IntSlice(actualUids)))
		})
	}
}

func (s *IntegrationSuite) TestHints() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql       string
		args      []interface{}
		expectLen int
	}

	for _, it := range []tt{
		{"/*A! master */ SELECT * FROM student WHERE uid = 42 AND 1=2", nil, 0},
		{"/*A! slave */  SELECT * FROM student WHERE uid = ?", []interface{}{1}, 0},
		{"/*A! master */ SELECT * FROM student WHERE uid = ?", []interface{}{1}, 1},
		{"/*A! master */ SELECT * FROM student WHERE uid in (?)", []interface{}{1}, 1},
		{"/*A! master */ SELECT * FROM student where uid between 1 and 10", nil, 1},
		{"/*A! trace(faf96a9c3d2d697d79967d8e21d81a6c) */ SELECT * FROM student WHERE uid = 42 AND 1=2", nil, 0},
	} {
		t.Run(it.sql, func(t *testing.T) {
			// select from logical table
			rows, err := db.Query(it.sql, it.args...)
			assert.NoError(t, err, "should query from sharding table successfully")
			defer rows.Close()
			data, _ := utils.PrintTable(rows)
			assert.Equal(t, it.expectLen, len(data))
		})
	}
}

func (s *IntegrationSuite) TestMultipleHints() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql       string
		args      []interface{}
		expectLen int
	}

	for _, it := range []tt{
		{"/*A! master */ /*A! fullscan */ SELECT * FROM student WHERE score > 100", nil, 0},
		{"/*A! slave */ /*A! master */ /*A! fullscan */ SELECT id,name FROM student WHERE score > 100", nil, 0},
		{"/*A! master */ /*A! direct */ SELECT * FROM student_0000 WHERE uid = ?", []interface{}{1}, 0},
		{"/*A! fullscan */ /*A! direct */ SELECT * FROM student WHERE uid in (?)", []interface{}{1}, 0},
	} {
		t.Run(it.sql, func(t *testing.T) {
			// select from logical table
			rows, err := db.Query(it.sql, it.args...)
			if err != nil {
				assert.True(t, strings.Contains(err.Error(), "hint type conflict"))
			} else {
				defer rows.Close()
				assert.NoError(t, err, "should query from sharding table successfully")
				data, _ := utils.PrintTable(rows)
				assert.Equal(t, it.expectLen, len(data))
			}
		})
	}
}

func (s *IntegrationSuite) TestShowCollation() {
	var (
		db = s.DB()
		t  = s.T()
	)

	result, err := db.Query("SHOW COLLATION")
	assert.NoErrorf(t, err, "show collation error: %v", err)

	defer result.Close()

	affected, err := result.ColumnTypes()
	assert.NoErrorf(t, err, "show collation: %v", err)
	assert.Equal(t, affected[0].DatabaseTypeName(), "VARCHAR")
}

func (s *IntegrationSuite) TestShowStatus() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql     string
		expectF func(t *testing.T, data [][]string) bool
	}

	for _, it := range []tt{
		{"SHOW STATUS", func(t *testing.T, data [][]string) bool {
			return len(data) > 0
		}},
		{"SHOW STATUS LIKE 'Key%';", func(t *testing.T, data [][]string) bool {
			return len(data) >= 5
		}},
	} {
		t.Run(it.sql, func(t *testing.T) {
			// show table status
			rows, err := db.Query(it.sql)
			assert.NoError(t, err, "should query status successfully")
			defer rows.Close()
			data, _ := utils.PrintTable(rows)
			assert.True(t, it.expectF(t, data))
		})
	}
}

func (s *IntegrationSuite) TestInsertAutoIncrement() {
	var (
		db = s.DB()
		t  = s.T()
	)

	defer func() {
		if _, err := db.Exec("DELETE FROM student"); err != nil {
			t.Fatal(err)
		}
	}()

	odd := 0
	even := 0

	for i := 0; i < 1000; i++ {
		result, err := db.Exec(
			`INSERT INTO student(uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?)`,
			100+i,
			rand2.Int31n(100),
			fmt.Sprintf("auto_increment_%d", i),
			fmt.Sprintf("auto_increment_%d", i),
			1,
			2022+i,
		)
		assert.NoErrorf(t, err, "insert row error: %+v", err)

		affected, err := result.RowsAffected()
		assert.NoErrorf(t, err, "insert row error: %+v", err)
		assert.True(t, affected == 1)

		lastId, err := result.LastInsertId()
		assert.NoErrorf(t, err, "insert row error: %+v", err)
		assert.True(t, lastId != 0, fmt.Sprintf("LastInsertId : %d", lastId))

		if lastId%2 == 0 {
			even++
		} else {
			odd++
		}

		assert.False(t, odd == 0, "sequence val all even number")
	}
}

func (s *IntegrationSuite) TestHintsRoute() {
	var (
		db = s.DB()
		t  = s.T()
	)
	type tt struct {
		sqlHint string
		sql     string
		same    bool
	}

	for _, it := range []tt{
		// use route hint
		{
			"/*A! route(employees_0000.student_0000,employees_0000.student_0007) */ SELECT * FROM student",
			"SELECT * FROM student", false,
		},
		// not use route hint, one shard
		{
			"/*A! route(employees_0000.student_0000,employees_0000.student_0007) */ SELECT * FROM student where uid=1",
			"SELECT * FROM student where uid=1", false,
		},
		// not use route hint, fullScan
		{
			"/*A! route(employees_0000.student_0000,employees_0000.student_0007) */ SELECT * FROM student  where uid>=1",
			"SELECT * FROM student  where uid>=1", false,
		},
	} {
		t.Run(it.sql, func(t *testing.T) {
			// select from logical table
			rows, err := db.Query(it.sqlHint)
			assert.NoError(t, err, "should query from sharding table successfully")
			defer rows.Close()
			data, _ := utils.PrintTable(rows)

			rows, err = db.Query(it.sql)
			assert.NoError(t, err, "should query from sharding table successfully")
			defer rows.Close()
			data2, _ := utils.PrintTable(rows)
			assert.Equal(t, it.same, len(data) == len(data2))
		})
	}
}

func (s *IntegrationSuite) TestShowTableStatus() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"SHOW TABLE STATUS FROM employees"},
		{"SHOW TABLE STATUS FROM employees WHERE name='student'"},
		{"SHOW TABLE STATUS FROM employees LIKE '%stu%'"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

func (s *IntegrationSuite) TestShowCharacterSet() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"SHOW CHARACTER SET;"},
		{"SHOW CHARACTER SET LIKE '%utf%'"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

func (s *IntegrationSuite) TestSetVariable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql  string
		flag bool
	}

	for _, it := range [...]tt{
		{sql: "SET @t1=1;", flag: true},
		{sql: "SET @t1=2,@t2='arana'", flag: true},
		{sql: "SET @@t1=4,@t2='arana'"},
		{sql: "SET @@t1=4,@@t2='arana'"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			_, err := db.Exec(it.sql)
			assert.Equal(t, err == nil, it.flag)
		})
	}
}

// TestAnalyzeTable
func (s *IntegrationSuite) TestAnalyzeTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"Analyze table student"},
		{"Analyze table student, departments"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

// TestCheckTable
func (s *IntegrationSuite) TestCheckTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"CHECK TABLE student"},
		{"CHECK TABLE student,departments"},
		{"CHECK TABLE student QUICK"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

// TestOptimizeTable
func (s *IntegrationSuite) TestOptimizeTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"Optimize table employees"},
		{"Optimize table student, departments"},
		{"Optimize LOCAL table student, departments"},
		{"Optimize NO_WRITE_TO_BINLOG table student, departments"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

// TestRenameTable
func (s *IntegrationSuite) TestRenameTable() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"RENAME TABLE student TO student_new"},
		{"RENAME TABLE student TO student_new, employees TO employees_new"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

func (s *IntegrationSuite) TestCompat80() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"select @@query_cache_size,@@query_cache_type,@@tx_isolation"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

func (s *IntegrationSuite) TestShowProcessList() {
	var (
		db = s.DB()
		t  = s.T()
	)

	type tt struct {
		sql string
	}

	for _, it := range [...]tt{
		{"show processlist"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			rows, err := db.Query(it.sql)
			assert.NoError(t, err)
			defer rows.Close()
		})
	}
}

func (s *IntegrationSuite) TestShowReplicaStatus() {
	sql_ := "SHOW REPLICA STATUS"
	t := s.T()
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql_, "", "")
	assert.Nil(t, err)
	assert.NotNil(t, stmtNodes)
}

func (s *IntegrationSuite) TestKill() {
	var (
		db = s.DB()
		t  = s.T()
	)

	// 1. get a process id
	rows, err := db.Query("SHOW PROCESSLIST")
	assert.NoError(t, err)
	defer rows.Close()
	data, _ := utils.PrintTable(rows)
	row := len(data)

	// 2. kill the last process
	_, err = db.Query(fmt.Sprintf("KILL %s", data[row-1][0]))
	assert.NoError(t, err)
}
