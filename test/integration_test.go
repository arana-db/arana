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
	"testing"
	"time"
)

import (
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/go-xorm/xorm"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	engine, err := xorm.NewEngine("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	if err != nil {
		t.Errorf("connection error: %v", err)
		return
	}

	result, err := engine.Exec(`INSERT INTO employees ( emp_no, birth_date, first_name, last_name, gender, hire_date )
		VALUES (?, ?, ?, ?, ?, ?)`, 100001, "1949-10-01", "共和国", "中华人民", "M", "1949-10-01")
	if err != nil {
		t.Errorf("insert row error: %v", err)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Errorf("insert row error: %v", err)
		return
	}
	assert.Equal(t, int64(1), affected)
}

func TestSelect(t *testing.T) {
	var v = &struct {
		EmpNo     int       `gorm:"emp_no"`
		BirthDate time.Time `gorm:"birth_date"`
		FirstName string    `gorm:"first_name"`
		LastName  string    `gorm:"last_name"`
		Gender    string    `gorm:"gender"`
		HireDate  time.Time `gorm:"hire_date"`
	}{}
	engine, err := xorm.NewEngine("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	if err != nil {
		t.Errorf("connection error: %v", err)
		return
	}

	result, err := engine.SQL(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees 
		WHERE emp_no = ?`, 100001).Get(v)
	if err != nil {
		t.Errorf("select row error: %v", err)
		return
	}
	assert.Equal(t, true, result)
	assert.Equal(t, "共和国", v.FirstName)
}

func TestUpdate(t *testing.T) {
	engine, err := xorm.NewEngine("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	if err != nil {
		t.Errorf("connection error: %v", err)
		return
	}

	result, err := engine.Exec(`UPDATE employees set last_name = ? where emp_no = ?`, "伟大的中华人民", 100001)
	if err != nil {
		t.Errorf("update row error: %v", err)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Errorf("update row error: %v", err)
		return
	}
	assert.Equal(t, int64(1), affected)
}

func TestDelete(t *testing.T) {
	engine, err := xorm.NewEngine("mysql", "dksl:123456@tcp(127.0.0.1:13306)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8")
	if err != nil {
		t.Errorf("connection error: %v", err)
		return
	}

	result, err := engine.Exec(`DELETE FROM employees WHERE emp_no = ?`, 100001)
	if err != nil {
		t.Errorf("delete row error: %v", err)
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Errorf("delete row error: %v", err)
		return
	}
	assert.Equal(t, int64(1), affected)
}
