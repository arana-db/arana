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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
)

func TestAggregateVisitor(t *testing.T) {
	type tt struct {
		sql string // input sql
		cnt int    // aggregate function amount
	}

	for _, it := range []tt{
		{"select id,name from t", 0},
		{"select count(*) from t", 1},
		{"select avg(age) from t", 2},
		{"select avg(age)+1 from t", 2},
		{"select count(*)+1 from t", 1},
		{"select sum(age)/count(age) from t", 2},
		{"select id,avg(age)+1,count(*),min(age),max(age) from t", 5},
		{"select CONCAT('total:',count(*)) from t", 1},
	} {
		t.Run(it.sql, func(t *testing.T) {
			var av aggregateVisitor
			_, stmt, err := ast.ParseSelect(it.sql)
			assert.NoError(t, err)
			_, err = stmt.Accept(&av)
			assert.NoError(t, err)
			assert.Len(t, av.aggregations, it.cnt)
		})
	}
}
