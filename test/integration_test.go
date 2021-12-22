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
	"testing"
	"time"
)

import (
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	db, err := sql.Open("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	result, err := db.Exec(`INSERT INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date )
		VALUES (?, ?, ?, ?, ?, ?)`, 100001, "1949-10-01", "共和国", "中华人民", "M", "1949-10-01")
	assert.NoErrorf(t, err, "insert row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "insert row error: %v", err)
	assert.Equal(t, int64(1), affected)
}

func TestSelect(t *testing.T) {
	db, err := sql.Open("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
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
	assert.Equal(t, "共和国", firstName)
}

func TestSelectLimit1(t *testing.T) {
	db, err := sql.Open("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
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
	assert.Equal(t, "共和国", firstName)
}

func TestUpdate(t *testing.T) {
	db, err := sql.Open("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	result, err := db.Exec(`UPDATE employees set last_name = ? where emp_no = ?`, "伟大的中华人民", 100001)
	assert.NoErrorf(t, err, "update row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "update row error: %v", err)

	assert.Equal(t, int64(1), affected)
}

func TestDelete(t *testing.T) {
	db, err := sql.Open("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	assert.NoErrorf(t, err, "connection error: %v", err)
	defer db.Close()

	result, err := db.Exec(`DELETE FROM employees WHERE emp_no = ?`, 100001)
	assert.NoErrorf(t, err, "delete row error: %v", err)
	affected, err := result.RowsAffected()
	assert.NoErrorf(t, err, "delete row error: %v", err)
	assert.Equal(t, int64(1), affected)
}
