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
	// 1. select statement
	stmt, err := Parse("select * from student as foo where `name` = if(1>2, 1, 2)")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", stmt)

	// 2. delete statement
	deleteStmt, err := Parse("delete from student as foo where `name` = if(1>2, 1, 2)")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", deleteStmt)

	insertStmtWithSetClause, err := Parse("insert into sink set a=77, b='88'")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithSetClause)

	insertStmtWithValues, err := Parse("insert into sink values(1, '2')")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithValues)
}
