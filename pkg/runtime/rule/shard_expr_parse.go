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
	"strings"
	"text/scanner"
)

// ---- lexer ----
// This lexer is similar to the one described in Chapter 13.
type lexer struct {
	scan  scanner.Scanner
	token rune // current lookahead token
}

func (lex *lexer) move() {
	lex.token = lex.scan.Scan()
	// fmt.Printf("after move, current token %q\n", lex.text())
}

func (lex *lexer) text() string { return lex.scan.TokenText() }
func (lex *lexer) peek() rune   { return lex.scan.Peek() }

// describe returns a string describing the current token, for use in errors.
func (lex *lexer) describe() string {
	switch lex.token {
	case scanner.EOF:
		return "end of file"
	case scanner.Ident:
		return fmt.Sprintf("identifier %s", lex.text())
	case scanner.Int, scanner.Float:
		return fmt.Sprintf("number %s", lex.text())
	}
	return fmt.Sprintf("%q", rune(lex.token)) // any other rune
}

func precedence(op rune) int {
	switch op {
	case '*', '/', '%':
		return 2
	case '+', '-':
		return 1
	}
	return 0
}

// ---- parser ----

// Parse parses the input string as an arithmetic expression.
//
//   expr = num                         a constant number, e.g., 3.14159
//        | id                          a variable name, e.g., x
//        | id '(' expr ',' ... ')'     a function
//        | '-' expr                    a unary operator (+-)
//        | expr '+' expr               a binary operator (+-*/)
//
func Parse(input string) (_ Expr, vars []Var, rerr error) {
	defer func() {
		switch x := recover().(type) {
		case nil:
			// no panic
		case string:
			rerr = fmt.Errorf("%s", x)
		default:
			rerr = fmt.Errorf("unexpected panic: resume state of panic")
		}
	}()

	lex := new(lexer)
	lex.scan.Init(strings.NewReader(input))
	lex.scan.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanFloats | scanner.ScanStrings
	lex.move() // initial lookahead
	_, e, err := parseExpr(lex, nil)
	if err != nil {
		return nil, nil, err
	}
	if lex.token != scanner.EOF {
		return nil, nil, fmt.Errorf("unexpected %s", lex.describe())
	}

	// parse vars
	l := len(input)
	for i := 0; i < l; i++ {
		if input[i] != '#' {
			continue
		}
		i++ // consume '#'

		var s string
		for {
			if input[i] == '#' {
				i++ // consume '#'
				break
			}
			if l <= i {
				break
			}
			s += string(input[i])
			i++
		}
		vars = append(vars, Var(s))
	}

	return e, vars, nil
}

func parseExpr(lex *lexer, s *stack) (*stack, Expr, error) { return parseBinary(lex, 1, s) }

// binary = unary ('+' binary)*
// parseBinary stops when it encounters an
// operator of lower precedence than prec1.
func parseBinary(lex *lexer, prec1 int, s *stack) (*stack, Expr, error) {
	s1, lhs, err := parseUnary(lex, s)
	if err != nil {
		return s, nil, err
	}
	s = s1

	for prec := precedence(lex.token); prec >= prec1; prec-- {
		for precedence(lex.token) == prec {
			op := lex.token
			lex.move() // consume operator
			s1, rhs, err := parseBinary(lex, prec+1, s)
			if err != nil {
				return s1, nil, err
			}
			s = s1
			lhs = binaryExpr{op: op, x: lhs, y: rhs}
		}
	}
	return s, lhs, nil
}

// unary = '+' expr | primary
func parseUnary(lex *lexer, s *stack) (*stack, Expr, error) {
	if lex.token == '+' || lex.token == '-' {
		op := lex.token
		lex.move() // consume '+' or '-'
		s1, e, err := parseUnary(lex, s)
		return s1, unary{op, e}, err
	}
	return parsePrimary(lex, s)
}

// primary = id
//         | id '(' expr ',' ... ',' expr ')'
//         | num
//         | '(' expr ')'
func parsePrimary(lex *lexer, s *stack) (*stack, Expr, error) {
	switch lex.token {
	case scanner.Ident:
		id := lex.text()
		lex.move() // consume Ident
		if lex.token != '(' {
			return s, Var(id), nil
		}

		lex.move() // consume '('

		// vars []Var
		var args []Expr

		if lex.token != ')' {
			if s == (*stack)(nil) {
				s = NewStack()
			}
			s.push(function{fn: id})
			for {
				s1, e, err := parseExpr(lex, s)
				if err != nil {
					return s, nil, err
				}
				s = s1
				args = append(args, e)
				//if v, ok := e.(Var); ok {
				//	vars = append(vars, v)
				//}
				if lex.token != ',' {
					break
				}
				lex.move() // consume ','
			}
			if lex.token != ')' {
				return s, nil, fmt.Errorf("got %s, want ')'", lex.describe())
			}
		}
		lex.move() // consume ')'

		if s != (*stack)(nil) {
			e, notEmpty := s.pop()
			if notEmpty {
				// if lex.token == ')' && !tmpstack.isEmpty() {
				//if  lex.token == ',' {
				//	lex.move() // consume ","
				//}
				c, ok := e.(function)
				if ok {
					c.args = append(c.args, args...)
					// c.vars = append(c.vars, vars...)
					return s, c, nil
				} else {
					return s, nil, fmt.Errorf("illegal stack element %#v, type %T", e, e) // impossible
				}
			}
		}
		// return s, function{fn:id, args:args, vars: vars}, nil
		return s, function{fn: id, args: args}, nil

	case scanner.Int, scanner.Float:
		f, err := strconv.ParseFloat(lex.text(), 64)
		if err != nil {
			return s, nil, err
		}
		lex.move() // consume number
		return s, constant(f), nil

	case scanner.String:
		token := stringConstant(lex.text())
		lex.move()
		return s, token, nil

	case '(':
		lex.move() // consume '('
		s1, e, err := parseExpr(lex, s)
		if err != nil || lex.token != ')' {
			return s, nil, fmt.Errorf("err %s, got %s, want ')'", err, lex.describe())
		}
		s = s1
		lex.move() // consume ')'
		return s, e, nil

	case '\'':
		// 此处之所以先存下原 mode，只分析 strings，是在分析 TestParse 的 case "hash(concat(#uid#, '1'), 100)"
		// 这个例子时，不能正确分析 concat 的分隔符 '1'
		mode := lex.scan.Mode
		defer func() {
			lex.scan.Mode = mode
		}()
		lex.scan.Mode = scanner.ScanStrings
		lex.move() // consume \'

		var str string
		for {
			str += string(lex.token)
			lex.move()
			if lex.token == '\'' || lex.token == scanner.EOF {
				break
			}
		}
		if lex.token != '\'' {
			return s, nil, fmt.Errorf("parsing string with quote ', got illegal last token %s", lex.describe())
		}
		lex.move() // consume \'
		return s, stringConstant(str), nil

	case '#':
		mode := lex.scan.Mode
		defer func() {
			lex.scan.Mode = mode
		}()
		lex.scan.Mode = scanner.ScanStrings
		lex.move() // consume #'

		var str string
		for {
			str += string(lex.token)
			lex.move()
			if lex.token == '#' || lex.token == scanner.EOF {
				break
			}
		}
		if lex.token != '#' {
			return s, nil, fmt.Errorf("parsing string with quote #, got illegal last token %s", lex.describe())
		}
		lex.move() // consume #
		return s, Var(str), nil
	}

	return s, nil, fmt.Errorf("unexpected %s", lex.describe())
}
