//
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

package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

import (
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/util/rand2"
	utils "github.com/arana-db/arana/pkg/util/tableprint"
)

const (
	driverName string = "mysql"

	// user:password@tcp(127.0.0.1:3306)/dbName?
	dataSourceName string = "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8"
)

func TestBasicTx(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

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

func TestSimpleSharding(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

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
		{"SELECT * FROM student", nil, total},
	} {
		t.Run(it.sql, func(t *testing.T) {
			// select from logical table
			rows, err := db.Query(it.sql, it.args...)
			assert.NoError(t, err, "should query from sharding table successfully")
			data, _ := utils.PrintTable(rows)
			assert.Equal(t, it.expectLen, len(data))
		})
	}
}

func TestInsert(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	result, err := db.Exec(`INSERT INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date )
		VALUES (?, ?, ?, ?, ?, ?)`, 100001, "1992-01-07", "scott", "lewis", "M", "2014-09-01")
	assert.NoErrorf(t, err, "insert row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)
	assert.Equal(t, int64(1), affected)
}

func TestSelect(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	rows, err := db.Query(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees 
		WHERE emp_no = ?`, 100001)
	assert.NoErrorf(t, err, "select row error: %v", err)

	var empNo string
	var birthDate time.Time
	var firstName string
	var lastName string
	var gender string
	var hireDate time.Time
	if rows.Next() {
		err = rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", firstName)
}

func TestSelectLimit1(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	rows, err := db.Query(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees LIMIT 1`)
	assert.NoErrorf(t, err, "select row error: %v", err)

	var empNo string
	var birthDate time.Time
	var firstName string
	var lastName string
	var gender string
	var hireDate time.Time
	if rows.Next() {
		err = rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", firstName)
}

func TestUpdate(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	result, err := db.Exec(`UPDATE employees set last_name = ? where emp_no = ?`, "louis", 100001)
	assert.NoErrorf(t, err, "update row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "update row error: %v", err)

	assert.Equal(t, int64(1), affected)
}

func TestDelete(t *testing.T) {
	db, err := sql.Open(driverName, dataSourceName)
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	result, err := db.Exec(`DELETE FROM employees WHERE emp_no = ?`, 100001)
	assert.NoErrorf(t, err, "delete row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "delete row error: %v", err)
	assert.Equal(t, int64(1), affected)
}
