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
	"fmt"
	"testing"
	"time"
)

import (
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
		//WithDevMode(), // NOTICE: UNCOMMENT IF YOU WANT TO DEBUG LOCAL ARANA SERVER!!!
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
			2022,
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
     values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE nickname='dump' `, 1654008174496657000, i, 3.14, fmt.Sprintf("fake_name_%d", i), fmt.Sprintf("fake_nickname_%d", i), 1, 2022)
	assert.NoErrorf(t, err, "insert row error: %v", err)
	_, err = result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)

	_, err = db.Exec(`INSERT IGNORE INTO student(id,uid,score,name,nickname,gender,birth_year) 
     values (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE uid=32 `, 1654008174496657000, i, 3.14, fmt.Sprintf("fake_name_%d", i), fmt.Sprintf("fake_nickname_%d", i), 1, 2022)
	assert.Error(t, err, "insert row error: %v", err)

}
func (s *IntegrationSuite) TestSelect() {
	var (
		db = s.DB()
		t  = s.T()
	)

	rows, err := db.Query(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees 
		WHERE emp_no = ?`, 100001)
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

	//drop table  physical name != logical name  and  physical name = logical name
	result, err := db.Exec(`DROP TABLE student,salaries`)

	assert.NoErrorf(t, err, "drop table error:%v", err)
	affected, err := result.RowsAffected()
	assert.Equal(t, int64(0), affected)
	assert.NoErrorf(t, err, "drop table  error: %v", err)

	//drop again, return error
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
		//shard  & no shard
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
	//with where
	_, err := db.Query(`select * from student  join titles on student.id=titles.emp_no where student.id=? and titles.emp_no=?`, 1, 2)
	assert.NoErrorf(t, err, "join table error:%v", err)
}

func (s *IntegrationSuite) TestShardingAgg() {
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
			2022,
		)
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
		assert.Equal(t, 3.14, cnt)
	})

	t.Run("SUM", func(t *testing.T) {
		row := db.QueryRow("select sum(score) as ttt from student")
		var cnt float64
		assert.NoError(t, row.Scan(&cnt))
		assert.Equal(t, int64(405), int64(cnt))
	})

	result, err := db.Exec(
		`INSERT IGNORE INTO student(id,uid,score,name,nickname,gender,birth_year) values (?,?,?,?,?,?,?)`,
		time.Now().UnixNano(),
		9527,
		100,
		"jason",
		"jason",
		1,
		2022,
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
		2022,
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
