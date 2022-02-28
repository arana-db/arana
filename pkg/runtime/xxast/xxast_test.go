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

package xxast

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	var (
		stmt Statement
		err  error
	)

	for _, sql := range []string{
		"select connection_id()",
		"select 1",
		"select * from student as foo where `name` = if(1>2, 1, 2)",
		"select * from employees limit 1",
		`SELECT CONCAT("'", user, "'@'",host,"'") FROM mysql.user`,
		"select * from student where uid = abs(-11)",
		"select * from student where uid = 1 limit 3 offset ?",
		"select case count(*) when 0 then -3.14 else 2.17 end as xxx from student where uid in (-1,-2,-3)",
		"select * from tb_user a where (uid >= ? AND uid <= ?)",
		"SELECT (2021 - birth_year) as AGE, count(1) as amount from student where uid between 1 and 10 group by (2021-birth_year)",
		"select * from student where uid = !0",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err = Parse(sql)
			assert.NoError(t, err)
			t.Log("stmt:", stmt)
		})
	}

	// 1. select statement
	stmt, err = Parse("select * from student as foo where `name` = if(1>2, 1, 2) order by age")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", stmt)

	// 2. delete statement
	deleteStmt, err := Parse("delete from student as foo where `name` = if(1>2, 1, 2)")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", deleteStmt)

	// 3. insert statements
	insertStmtWithSetClause, err := Parse("insert into sink set a=77, b='88'")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithSetClause)

	insertStmtWithValues, err := Parse("insert into sink values(1, '2')")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithValues)

	insertStmtWithOnDuplicateUpdates, err := Parse(
		"insert into sink (a, b) values(1, '2') on duplicate key update a=a+1",
	)
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithOnDuplicateUpdates)

	// 4. update statement
	updateStmt, err := Parse(
		"update source set a=a+1, b=b+2 where a>1 order by a limit 5",
	)
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", updateStmt)
}
