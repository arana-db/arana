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

package rule

import (
	"fmt"
	"strconv"
	"testing"
)

func TestParse(t *testing.T) {
	//assert.True(t, preParse(expString) == expression)
	tests := []struct {
		expr string
		env  Env
		want string
	}{
		{"__SHARD__.hash(__FUNC__.toint(__FUNC__.substr(#uid#, 1, 2)), 100)", Env{"uid": "87616"}, "87"},
		{"__SHARD__.hash(__FUNC__.concat(#uid#, '1'), 100)", Env{"uid": "87616"}, "61"},
		{"__FUNC__.div(__FUNC__.substr(#uid#, 2), 10)", Env{"uid": "87616"}, "761.6"},
	}
	var prevExpr string
	for _, test := range tests {
		// Print expr only when it changes.
		if test.expr != prevExpr {
			fmt.Printf("\n%s\n", test.expr)
			prevExpr = test.expr
		}
		expr, vars, err := Parse(test.expr)
		if err != nil {
			t.Error(err) // parse error
			continue
		}
		if len(vars) != 1 || vars[0] != "uid" {
			t.Errorf("illegal vars %#v", vars)
		}

		// got := fmt.Sprintf("%.6g", )
		evalRes, _ := expr.Eval(test.env)
		evalResFloat, _ := strconv.ParseFloat(evalRes.String(), 64)
		got := fmt.Sprintf("%.6g", evalResFloat)
		fmt.Printf("\t%v => %s\n", test.env, got)
		if got != test.want {
			t.Errorf("%s.Eval() in %v = %q, want %q\n", test.expr, test.env, got, test.want)
		}
	}
}

//
//func TestFormat(t *testing.T) {
//	var str = "hash(toint(substr(var1, 1, 2)), 100)"
//	exp, _ := parser.ParseExpr(str)
//	ast.Inspect(exp, func(n ast.Node) bool {
//		// Find Return Statements
//		_, ok := n.(*ast.ReturnStmt)
//		if ok {
//			fmt.Printf("return statement found on line\n")
//			return true
//		}
//
//		// Find Functions
//		fn, ok := n.(*ast.FuncDecl)
//		if ok {
//			var exported string
//			if fn.Name.IsExported() {
//				exported = "exported "
//			}
//			fmt.Printf("%s function declaration found on line", exported)
//			return true
//		}
//
//		return true
//	})
//}
