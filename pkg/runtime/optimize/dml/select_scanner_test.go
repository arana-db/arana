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

func TestSelectScanner_Scan(t *testing.T) {
	type tt struct {
		sql      string
		selects  []string
		orderBys []string
	}

	for _, it := range []tt{
		{
			"select id,name,age from student order by age desc",
			[]string{"`id`", "`name`", "`age`"},
			[]string{"`age`"},
		},
		{
			"select id,name from student order by age desc",
			[]string{"`id`", "`name`", "`age`"},
			[]string{"`age`"},
		},
		{
			"select id,name,score from student order by age desc,score",
			[]string{"`id`", "`name`", "`score`", "`age`"},
			[]string{"`age`", "`score`"},
		},
		{
			"select id,name,2022-birth_year as age from student order by age",
			[]string{"`id`", "`name`", "2022-`birth_year` AS `age`"},
			[]string{"2022-`birth_year` AS `age`"},
		},
		{
			"select id,name from student order by 2022-birth_year",
			[]string{"`id`", "`name`", "2022-`birth_year`"},
			[]string{"2022-`birth_year`"},
		},
	} {
		t.Run(it.sql, func(t *testing.T) {
			_, stmt, _ := ast.ParseSelect(it.sql)
			var result selectResult
			scanner := newSelectScanner(stmt, nil)
			err := scanner.scan(&result)
			assert.NoError(t, err)
			assert.Len(t, stmt.Select, len(it.selects))
			assert.Len(t, result.orders, len(it.orderBys))

			var (
				rf      ast.RestoreFlag
				selects []string
				orders  []string
			)
			for i := range stmt.Select {
				selects = append(selects, ast.MustRestoreToString(rf, stmt.Select[i]))
			}
			for i := range result.orders {
				orders = append(orders, ast.MustRestoreToString(rf, result.orders[i]))
			}
			assert.Equal(t, it.selects, selects)
			assert.Equal(t, it.orderBys, orders)
		})
	}
}
