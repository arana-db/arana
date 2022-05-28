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
	"math"
	"strconv"
	"testing"
	"text/scanner"
)

//!+Eval
func TestNumEval(t *testing.T) {
	piStr := fmt.Sprintf("%f", math.Pi)
	tests := []struct {
		expr string
		env  Env
		want string
	}{
		{"add(A, B)", Env{"A": "8", "B": "4"}, "12"},
		{"sub(A, B)", Env{"A": "8", "B": "4"}, "4"},
		{"mul(A, B)", Env{"A": "8", "B": "4"}, "32"},
		{"div(A, B)", Env{"A": "8", "B": "4"}, "2"},
		{"sqrt(A / pi)", Env{"A": "87616", "pi": Value(piStr)}, "167"},
		{"testload(x)", Env{"x": "dbc"}, "312"},
		{"testload(x)", Env{"x": "DBc"}, "312"},
		{"substr(x, '1')", Env{"x": "1234"}, "1234"},
		{"substr(x, '1', '2')", Env{"x": "1234"}, "12"},
		{"pow(x, 3) + pow(y, 3)", Env{"x": "9", "y": "10"}, "1729"},
		{"+1", Env{}, "1"},
		{"5 / 9 * (F - 32)", Env{"F": "-40"}, "-40"},
		{"5 / 9 * (F - 32)", Env{"F": "32"}, "0"},
		{"5 / 9 * (F - 32)", Env{"F": "212"}, "100"},
		{"-1 + -x", Env{"x": "1"}, "-2"},
		{"-1 - x", Env{"x": "1"}, "-2"},
	}
	var prevExpr string
	for _, test := range tests {
		// Print expr only when it changes.
		if test.expr != prevExpr {
			fmt.Printf("\n%s\n", test.expr)
			prevExpr = test.expr
		}
		expr, _, err := Parse(test.expr)
		if err != nil {
			t.Error(err) // parse error
			continue
		}
		// got := fmt.Sprintf("%.6g", )
		evalRes, _ := expr.Eval(test.env)
		evalResFloat, _ := strconv.ParseFloat(evalRes.String(), 64)
		got := fmt.Sprintf("%.6g", evalResFloat)
		fmt.Printf("\t%v => %s\n", test.env, got)
		if got != test.want {
			t.Errorf("%s.Eval() in %v = %q, want %q\n",
				test.expr, test.env, got, test.want)
		}
	}

	v := Var("hello")
	env := Env{"hello": "hello"}
	vv, err := v.Eval(env)
	if vv != "hello" || err != nil {
		t.Errorf("v %v Eval(env:%v) = {value:%v, error:%v}", v, env, vv, err)
	}

	sv := stringConstant("hello")
	vv, err = sv.Eval(nil)
	if vv != "hello" || err != nil {
		t.Errorf("v %v Eval(env:%v) = {value:%v, error:%v}", v, env, vv, err)
	}

	splitExpr, _, err := Parse("split(x, '|', 2)")
	if err != nil {
		t.Errorf("Parse('split(x, y, 2)') = error %v", err)
	}
	splitRes, err := splitExpr.Eval(Env{"x": "abc|de|f"})
	if err != nil || splitRes != "de" {
		t.Errorf("{'split(x, y, 2)', '|', 2} = {res %v, error %v}", splitRes, err)
	}
}

func TestErrors(t *testing.T) {
	for _, test := range []struct{ expr, wantErr string }{
		{"x $ 2", "unexpected '$'"},
		{"math.Pi", "unexpected '.'"},
		{"!true", "unexpected '!'"},
		//{`"hello"`, "hello"},
		{"log(10)", `unknown function "log"`},
		{"sqrt(1, 2)", "illegal args number 2 of func sqrt, want 1"},
	} {
		expr, _, err := Parse(test.expr)
		if err == nil {
			vars := make(map[Var]bool)
			err = expr.Check(vars)
			if err == nil {
				t.Errorf("unexpected success: %s", test.expr)
				continue
			}
		}
		fmt.Printf("%-20s%v\n", test.expr, err) // (for book)
		if err.Error() != test.wantErr {
			t.Errorf("got error \"%s\", want %s", err, test.wantErr)
		}
	}
}

func TestCheck(t *testing.T) {
	sc := stringConstant("hello")
	err := sc.Check(nil)
	if err != nil {
		t.Fatalf("stringConstant check() result %v != nil", err)
	}

	c := function{
		fn:   "hash",
		args: nil,
	}
	err = c.Check(nil)
	if err == nil {
		t.Fatalf("function %#v check() result %v should not be nil", c, err)
	}

	c = function{
		fn:   "substr",
		args: nil,
	}
	err = c.Check(nil)
	if err == nil {
		t.Fatalf("function %#v check() result %v should be nil", c, err)
	}
	c.args = append(c.args, sc)
	//c.args = append(c.args, sc)
	//c.args = append(c.args, sc)
	err = c.Check(nil)
	if err == nil {
		t.Fatalf("function %#v check() result %v should not be nil", c, err)
	}

	piStr := fmt.Sprintf("%f", math.Pi)
	var tests = []struct {
		input string
		env   Env
		want  string // expected error from Parse/Check or result from Eval
	}{
		{"hello", nil, ""},
		{"+2", nil, ""},
		// {"x % 2", nil, "unexpected '%'"},
		{"x % 2", nil, ""},
		{"!true", nil, "unexpected '!'"},
		{"log(10)", nil, `unknown function "log"`},
		{"sqrt(1, 2)", nil, "illegal args number 2 of func sqrt, want 1"},
		{"sqrt(A / pi)", Env{"A": "87616", "pi": Value(piStr)}, "167"},
		{"pow(x, 3) + pow(y, 3)", Env{"x": "9", "y": "10"}, "1729"},
		{"5 / 9 * (F - 32)", Env{"F": "-40"}, "-40"},
	}

	for _, test := range tests {
		expr, _, err := Parse(test.input)
		if err == nil {
			err = expr.Check(map[Var]bool{})
		}
		if err != nil {
			if err.Error() != test.want {
				t.Errorf("%s: got %q, want %q", test.input, err, test.want)
			}
			continue
		}

		if test.want != "" {
			//got := fmt.Sprintf("%.6g", expr.Eval(test.env))
			evalRes, _ := expr.Eval(test.env)
			evalResFloat, _ := strconv.ParseFloat(evalRes.String(), 64)
			got := fmt.Sprintf("%.6g", evalResFloat)
			if got != test.want {
				t.Errorf("%s: %v => %s, want %s",
					test.input, test.env, got, test.want)
			}
		}
	}
}

func TestString(t *testing.T) {
	var e Expr

	e = Var("hello")
	t.Logf("%v string %s", e, e)

	e = constant(3.14)
	t.Logf("%v string %s", e, e)

	e = stringConstant("hello")
	t.Logf("%v string %s", e, e)

	e = unary{
		op: '-',
		x:  constant(1234),
	}
	t.Logf("%v string %s", e, e)

	e = binary{
		op: '+',
		x:  stringConstant("1"),
		y:  stringConstant("1"),
	}
	t.Logf("%v string %s", e, e)

	f := function{
		fn: "add",
	}
	f.args = append(f.args, stringConstant("1"))
	f.args = append(f.args, stringConstant("2"))
	e = f
	t.Logf("%v string %s", e, e)

	var l lexer
	l.token = scanner.EOF
	t.Logf("eof string %s", l.describe())
	l.token = scanner.Ident
	t.Logf("ident string %s", l.describe())
	l.token = scanner.Int
	t.Logf("int string %s", l.describe())

	l.token = scanner.String
	s, e, err := parsePrimary(&l, nil)
	if s != nil || err != nil || e == nil {
		t.Logf("parsePrimary() = {stack:%v, expression:%v error:%v}", s, e, err)
	}
}
