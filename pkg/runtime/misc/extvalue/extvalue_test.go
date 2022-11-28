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

package extvalue_test

import (
	"fmt"
	"testing"
)

import (
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
	_ "github.com/arana-db/arana/pkg/runtime/function"
	"github.com/arana-db/arana/pkg/runtime/misc/extvalue"
)

func TestCompute(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, next := range []tt{
		{"1+2", "3"},
		{"3 div 2", "1"},
		{"3/2", "1.5"},
		{"case 1 when 1 then 'ok' end", "ok"},
		{"case 1 when 2 then 'ok' end", "<nil>"},
		{"case when 2>1 then 'ok' end", "ok"},
		{"case when 0>-(3-1) then 'ok' end", "ok"},
	} {
		t.Run(next.input, func(t *testing.T) {
			expr, err := getExpr(next.input)
			assert.NoError(t, err)
			v, err := extvalue.Compute(expr, nil)
			assert.NoError(t, err)
			assert.Equal(t, next.expect, fmt.Sprint(v))
		})
	}
}

func getExpr(s string) (ast.ExpressionNode, error) {
	_, sel, _ := ast.ParseSelect("select " + s)
	switch f := sel.Select[0].(type) {
	case *ast.SelectElementExpr:
		return f.Expression(), nil
	case *ast.SelectElementFunction:
		switch v := f.Function().(type) {
		case *ast.Function, *ast.CaseWhenElseFunction:
			return &ast.PredicateExpressionNode{
				P: &ast.AtomPredicateNode{
					A: &ast.FunctionCallExpressionAtom{
						F: v,
					},
				},
			}, nil
		}
	}
	return nil, errors.Errorf("cannot extract expression from: %s", s)
}
