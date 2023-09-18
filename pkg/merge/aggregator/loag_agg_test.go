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

package aggregator

import (
	"testing"
)

import (
	"github.com/arana-db/parser"

	"github.com/stretchr/testify/assert"
)

import (
	rast "github.com/arana-db/arana/pkg/runtime/ast"
)

func TestLoadAgg(t *testing.T) {
	sql := "select max(age), min(age), sum(age), count(age) from student group by name"
	parser := parser.New()
	stmt, err := parser.ParseOneStmt(sql, "", "")
	assert.NoError(t, err)

	rstmt, err := rast.FromStmtNode(stmt)
	assert.NoError(t, err)

	aggs := LoadAggs(rstmt.(*rast.SelectStatement).Select)
	assert.Equal(t, 4, len(aggs))

	_, ok := aggs[0]().(*MaxAggregator)
	assert.True(t, ok)
	_, ok = aggs[1]().(*MinAggregator)
	assert.True(t, ok)
	_, ok = aggs[2]().(*AddAggregator)
	assert.True(t, ok)
	_, ok = aggs[3]().(*AddAggregator)
	assert.True(t, ok)
}
