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

package optimize_test

import (
	"github.com/arana-db/arana/pkg/proto"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
	_ "github.com/arana-db/arana/pkg/runtime/function"
	. "github.com/arana-db/arana/pkg/runtime/optimize"
)

func TestCalculVisitor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type tt struct {
		sql    string
		expect string
	}

	for _, it := range []tt{
		{"select 1+1", "2"},
		{"select 1+ABS(-1.2)", "2.2"},
		{"select ABS(-1.2) + 1 + EXP(-3.14) ", "2.243282797901965896"},
	} {
		t.Run(it.sql, func(t *testing.T) {
			_, rawStmt := ast.MustParse(it.sql)
			stmt := rawStmt.(*ast.SelectStatement)

			cal := NewXCalcualtor(nil)

			calculateRes, errtmp := stmt.Select[0].(*ast.SelectElementExpr).Accept(cal)
			assert.NoError(t, errtmp)
			assert.Equal(t, it.expect, calculateRes.(proto.Value).String())
		})
	}
}
