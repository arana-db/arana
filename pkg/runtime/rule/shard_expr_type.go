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
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
)

import (
	"github.com/dubbogo/gost/math/big"
)

const (
	defaultValue = ""
)

// An Expr is an arithmetic/string expression.
type Expr interface {
	// Eval returns the value of this Expr in the environment env.
	Eval(env Env) (Value, error)
	// Check reports errors in this Expr and adds its Vars to the set.
	Check(vars map[Var]bool) error
	// String output
	String() string
}

type (
	// A Var identifies a variable, e.g., x.
	Var string
	Env map[Var]Value
)

func (v Var) Eval(env Env) (Value, error) {
	return env[v], nil
}

func (v Var) Check(vars map[Var]bool) error {
	vars[v] = true
	return nil
}

func (v Var) String() string {
	var buf bytes.Buffer
	write(&buf, v)
	return buf.String()
}

// Value defines var value
type Value string

func (v Value) String() string {
	return string(v)
}

func (v Value) ToIntString() string {
	//pos := strings.LastIndex(string(v), ".")
	//if pos != -1 {
	//	return string(v)[:pos]
	//}
	//return string(v)
	decimal, _ := gxbig.NewDecFromString(string(v))
	decimal.Round(decimal, 0, gxbig.ModeHalfEven)
	return decimal.String()
}

// A constant is a numeric constant, e.g., 3.141.
type constant float64

func (c constant) Eval(Env) (Value, error) {
	return Value(strconv.Itoa(int(c))), nil
}

func (constant) Check(map[Var]bool) error {
	return nil
}

func (c constant) String() string {
	var buf bytes.Buffer
	write(&buf, c)
	return buf.String()
}

// A stringConstant is a string constant, e.g., 3.141.
type stringConstant string

func (s stringConstant) Eval(Env) (Value, error) {
	return Value(s), nil
}

func (stringConstant) Check(map[Var]bool) error {
	return nil
}

func (s stringConstant) String() string {
	return (string)(s)
}

// A unary represents a unary operator expression, e.g., -x.
type unary struct {
	op rune // one of '+', '-'
	x  Expr
}

func (u unary) Eval(env Env) (Value, error) {
	switch u.op {
	case '+':
		return u.x.Eval(env)

	case '-':
		xv, err := u.x.Eval(env)
		if err != nil {
			return defaultValue, err
		}
		x, err := gxbig.NewDecFromString(xv.String())
		if err != nil {
			return defaultValue, fmt.Errorf("invalid value %s", xv.String())
		}
		return Value("-" + x.String()), nil
	}

	return defaultValue, fmt.Errorf("unsupported unary operator: %q", u.op)
}

func (u unary) Check(vars map[Var]bool) error {
	if !strings.ContainsRune("+-", u.op) {
		return fmt.Errorf("unexpected unary op %q", u.op)
	}
	return u.x.Check(vars)
}

func (u unary) String() string {
	var buf bytes.Buffer
	write(&buf, u)
	return buf.String()
}

// A binary represents a binary operator expression, e.g., x+y.
type binaryExpr struct {
	op   rune // one of '+', '-', '*', '/', '%'
	x, y Expr
}

func (b binaryExpr) Eval(env Env) (Value, error) {
	var (
		err     error
		xv, yv  Value
		x, y, r *gxbig.Decimal
	)
	xv, err = b.x.Eval(env)
	if err != nil {
		return xv, err
	}
	x, err = gxbig.NewDecFromString(xv.String())
	if err != nil {
		return xv, fmt.Errorf("gxbig.NewDecFromString(xv: %v), got error: %w", xv, err)
	}

	yv, err = b.y.Eval(env)
	if err != nil {
		return yv, err
	}
	y, err = gxbig.NewDecFromString(yv.String())
	if err != nil {
		return yv, fmt.Errorf("gxbig.NewDecFromString(yv: %v), got error: %w", yv, err)
	}

	r = new(gxbig.Decimal)
	switch b.op {
	case '+':
		err = gxbig.DecimalAdd(x, y, r)
		if err != nil {
			return defaultValue, fmt.Errorf("gxbig.DecimalAdd(x: %v, y: %v), got error: %w", x, y, err)
		}

	case '-':
		err = gxbig.DecimalSub(x, y, r)
		if err != nil {
			return defaultValue, fmt.Errorf("gxbig.DecimalSub(x: %v, y: %v), got error: %w", x, y, err)
		}

	case '*':
		err = gxbig.DecimalMul(x, y, r)
		if err != nil {
			return defaultValue, fmt.Errorf("gxbig.DecimalMul(x: %v, y: %v), got error: %w", x, y, err)
		}

	case '/':
		err = gxbig.DecimalDiv(x, y, r, gxbig.DivFracIncr)
		if err != nil {
			return defaultValue, fmt.Errorf("gxbig.DecimalDiv(x: %v, y: %v), got error: %w", x, y, err)
		}

	case '%':
		err = gxbig.DecimalMod(x, y, r)
		if err != nil {
			return defaultValue, fmt.Errorf("gxbig.DecimalMod(x: %v, y: %v), got error: %w", x, y, err)
		}

	default:
		return defaultValue, fmt.Errorf("unsupported binary operator: %q", b.op)
	}

	return Value(r.String()), nil
}

func (b binaryExpr) String() string {
	var buf bytes.Buffer
	write(&buf, b)
	return buf.String()
}

func (b binaryExpr) Check(vars map[Var]bool) error {
	if !strings.ContainsRune("+-*/%", b.op) {
		return fmt.Errorf("unexpected binary op %q", b.op)
	}
	if err := b.x.Check(vars); err != nil {
		return err
	}
	return b.y.Check(vars)
}

// A function represents a function function expression, e.g., sin(x).
type function struct {
	fn   string // one of "pow", "sqrt"
	args []Expr
}

func (c function) Eval(env Env) (Value, error) {
	argsNum := len(c.args)
	if argsNum == 0 {
		return defaultValue, fmt.Errorf("args number is 0 of func %s", c.fn)
	}

	switch c.fn {
	case "toint":
		return c.args[0].Eval(env)

	case "hash":
		b := binaryExpr{
			op: '%',
			x:  c.args[0],
			y:  constant(2),
		}
		if len(c.args) == 2 {
			b.y = c.args[1]
		}
		return b.Eval(env)

	case "add":
		b := binaryExpr{
			op: '+',
			x:  c.args[0],
			y:  c.args[1],
		}
		return b.Eval(env)

	case "sub":
		b := binaryExpr{
			op: '-',
			x:  c.args[0],
			y:  c.args[1],
		}
		return b.Eval(env)

	case "mul":
		b := binaryExpr{
			op: '*',
			x:  c.args[0],
			y:  c.args[1],
		}
		return b.Eval(env)

	case "div":
		b := binaryExpr{
			op: '/',
			x:  c.args[0],
			y:  c.args[1],
		}
		return b.Eval(env)

	case "substr":
		v0, err := c.args[0].Eval(env)
		if err != nil {
			return defaultValue, err
		}
		str := v0.String()

		v1, err := c.args[1].Eval(env)
		if err != nil {
			return defaultValue, err
		}
		strlen := len(str)
		f, err := strconv.ParseFloat(v1.String(), 64)
		if err != nil {
			return defaultValue, fmt.Errorf("illegal args[1] %v of func substr", v1.String())
		}
		startPos := int(f)
		if startPos == 0 {
			return defaultValue, nil
		}
		if startPos < 0 {
			startPos += strlen
		} else {
			startPos--
		}
		if startPos < 0 || startPos >= strlen {
			return defaultValue, fmt.Errorf("illegal args[1] %v of func substr", v1.String())
		}

		endPos := strlen
		if len(c.args) == 3 {
			v2, err := c.args[2].Eval(env)
			if err != nil {
				return defaultValue, err
			}
			l, err := strconv.Atoi(v2.String())
			if err != nil {
				return defaultValue, err
			}
			if l < 1 {
				return defaultValue, fmt.Errorf("illegal args[2] %v of func substr", v2.String())
			}
			if startPos+l < endPos {
				endPos = startPos + l
			}
		}

		return Value(str[startPos:endPos]), nil

	case "concat":
		var builder strings.Builder
		for i := 0; i < len(c.args); i++ {
			v, err := c.args[i].Eval(env)
			if err != nil {
				return defaultValue, err
			}

			builder.WriteString(v.String())
		}
		return Value(builder.String()), nil

	case "testload":
		v0, err := c.args[0].Eval(env)
		if err != nil {
			return defaultValue, err
		}

		s := v0.String()

		var b strings.Builder
		b.Grow(len(s))
		for i := 0; i < len(s); i++ {
			c := s[i]
			if 'a' <= c && c <= 'j' {
				c -= 'a' - '0'
			} else if 'A' <= c && c <= 'J' {
				c -= 'A' - '0'
			}

			b.WriteByte(c)
		}

		return Value(b.String()), nil

	case "split":
		v0, err := c.args[0].Eval(env)
		if err != nil {
			return defaultValue, err
		}

		v1, err := c.args[1].Eval(env)
		if err != nil {
			return defaultValue, err
		}

		v2, err := c.args[2].Eval(env)
		if err != nil {
			return defaultValue, err
		}
		pos, err := strconv.Atoi(v2.String())
		if err != nil {
			return defaultValue, err
		}
		if pos < 1 {
			return defaultValue, fmt.Errorf("illegal args[2] %v of func split", v2.String())
		}

		arr := strings.Split(v0.String(), v1.String())
		return Value(arr[pos-1]), nil

	case "pow":
		v0, err := c.args[0].Eval(env)
		if err != nil {
			return defaultValue, err
		}
		v0f, err := strconv.ParseFloat(v0.String(), 64)
		if err != nil {
			return defaultValue, err
		}

		v1, err := c.args[1].Eval(env)
		if err != nil {
			return defaultValue, err
		}
		v1f, err := strconv.ParseFloat(v1.String(), 64)
		if err != nil {
			return defaultValue, err
		}

		v := math.Pow(v0f, v1f)
		return Value(fmt.Sprintf("%f", v)), nil

	case "sqrt":
		v0, err := c.args[0].Eval(env)
		if err != nil {
			return defaultValue, err
		}
		v0f, err := strconv.ParseFloat(v0.String(), 64)
		if err != nil {
			return defaultValue, err
		}
		v := math.Sqrt(v0f)
		return Value(fmt.Sprintf("%f", v)), nil
	}

	return defaultValue, fmt.Errorf("unsupported function function: %s", c.fn)
}

func (c function) String() string {
	var buf bytes.Buffer
	write(&buf, c)
	return buf.String()
}

func (c function) Check(vars map[Var]bool) error {
	arity, ok := numParams[c.fn]
	if !ok {
		return fmt.Errorf("unknown function %q", c.fn)
	}
	if argsNum := len(c.args); argsNum != arity {
		if argsNum == 0 || arity < argsNum {
			return fmt.Errorf("illegal args number %d of func %s, want %d", argsNum, c.fn, arity)
		}
		switch c.fn {
		case "substr":
			if argsNum < 2 {
				return fmt.Errorf("illegal args number %d of func substr, want %d", argsNum, arity)
			}
		}
	}
	for _, arg := range c.args {
		if err := arg.Check(vars); err != nil {
			return err
		}
	}
	return nil
}

// expr stack
type stack []Expr

func NewStack() *stack {
	return &stack{}
}

// size: return the element number of @s
func (s *stack) size() int {
	return len(*s)
}

// isEmpty: check if stack is empty
func (s *stack) isEmpty() bool {
	return s.size() == 0
}

// push a new value onto the stack
func (s *stack) push(e Expr) *stack {
	*s = append(*s, e)
	return s
}

// pop: Remove and return top element of stack. Return false if stack is empty.
func (s *stack) pop() (_ Expr, notEmpty bool) {
	if s.isEmpty() {
		return nil, false
	}

	i := len(*s) - 1
	e := (*s)[i]
	*s = (*s)[:i]
	return e, true
}

var numParams = map[string]int{
	"toint":    1,
	"hash":     2,
	"add":      2,
	"sub":      2,
	"mul":      2,
	"div":      2,
	"substr":   3,
	"concat":   10,
	"testload": 1,
	"split":    3,
	"pow":      2,
	"sqrt":     1,
}

func write(buf *bytes.Buffer, e Expr) {
	switch e.(type) {
	case constant:
		fmt.Fprintf(buf, "%g", e.(constant))

	case stringConstant:
		fmt.Fprintf(buf, "%s", e.(stringConstant))

	case Var, *Var:
		v, ok := e.(Var)
		if !ok {
			v = *(e.(*Var))
		}

		fmt.Fprintf(buf, "%s", string(v))

	case unary, *unary:
		u, ok := e.(unary)
		if !ok {
			u = *(e.(*unary))
		}

		fmt.Fprintf(buf, "(%c", u.op)
		write(buf, u.x)
		buf.WriteByte(')')

	case binaryExpr, *binaryExpr:
		b, ok := e.(binaryExpr)
		if !ok {
			b = *(e.(*binaryExpr))
		}

		buf.WriteByte('(')
		write(buf, b.x)
		fmt.Fprintf(buf, " %c ", b.op)
		write(buf, b.y)
		buf.WriteByte(')')

	case function, *function:
		f, ok := e.(function)
		if !ok {
			f = *(e.(*function))
		}

		fmt.Fprintf(buf, "%s(", f.fn)
		for i, arg := range f.args {
			if i > 0 {
				buf.WriteString(", ")
			}
			write(buf, arg)
		}
		buf.WriteByte(')')

	default:
		fmt.Fprintf(buf, "unknown Expr: %T", e)
	}
}
