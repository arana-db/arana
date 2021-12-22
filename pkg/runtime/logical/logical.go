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

package logical

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dubbogo/arana/pkg/runtime/misc"
)

const (
	_and = "&&"
	_or  = "||"
	_not = "!"
)

const (
	_ Op = iota
	Land
	Lor
)

var (
	_ Logical = (*atom)(nil)
	_ Logical = (*composite)(nil)
)

// Op represents the logical operator includes AND, OR.
type Op uint8

func (o Op) String() string {
	switch o {
	case Land:
		return "AND"
	case Lor:
		return "OR"
	default:
		panic("unreachable")
	}
}

// Option represents the option of Logical.
type Option func(*atom)

// WithValue attaches a value into Logical.
func WithValue(value interface{}) Option {
	return func(a *atom) {
		a.value = value
	}
}

// WithSortKey sets the sortable key of Logical item.
func WithSortKey(key string) Option {
	return func(a *atom) {
		a.sk = key
	}
}

// New creates Logical.
func New(key string, options ...Option) Logical {
	ret := new(atom)
	for _, it := range options {
		it(ret)
	}
	ret.id = key
	return ret
}

// Logical 逻辑运算器, 最终优化目标消除内部并集, 也就是展开成
type Logical interface {
	fmt.Stringer
	// And 执行与运算
	And(other Logical) Logical
	// Not 执行非运算
	Not() Logical
	// Or 执行或运算
	Or(other Logical) Logical
	// ToString returns a display string.
	ToString(and, or string) string
	// phantom avoids the implementation out of current package.
	phantom()
}

type atom struct {
	not   bool
	id    string
	sk    string // sort key
	value interface{}
}

func (a *atom) phantom() {
}

func (a *atom) Not() Logical {
	b := new(atom)
	*b = *a
	b.not = !b.not
	return b
}

func (a *atom) ToString(_, _ string) string {
	return a.String()
}

func (a *atom) equalsWith(other Logical) bool {
	switch b := other.(type) {
	case *atom:
		return a.id == b.id && a.not == b.not
	}
	return false
}

func (a *atom) String() string {
	if a.not {
		return _not + a.id
	}
	return a.id
}

func (a *atom) And(other Logical) Logical {
	switch b := other.(type) {
	case *composite:
		switch b.op {
		case Land:
			// A ∩ ( A ∩ B ∩ C ) => A ∩ B ∩ C
			ret := &composite{op: Land}
			ret.ch = append(ret.ch, a)
			for _, it := range b.ch {
				if !a.equalsWith(it) {
					ret.ch = append(ret.ch, it)
				}
			}
			return ret
		case Lor:
			// A ∩ (B ∪ C) => (A ∩ B) ∪ (A ∩ C)
			// A ∩ (A ∪ B) => A
			for _, it := range b.ch {
				if next, ok := it.(*atom); ok && next == a {
					return a
				}
			}
			ret := &composite{op: Lor}
			for _, it := range b.ch {
				ret.ch = append(ret.ch, a.And(it))
			}
			return ret
		}
	case *atom:
		if a.equalsWith(b) {
			return a
		}
		reverse := strings.Compare(misc.FirstNonEmptyString(a.sk, a.id), misc.MustFirstNonEmptyString(b.sk, b.id)) == -1
		ret := &composite{op: Land}
		if reverse {
			ret.ch = []Logical{b, a}
		} else {
			ret.ch = []Logical{a, b}
		}
		return ret
	}
	panic("unreachable")
}

func (a *atom) Or(other Logical) Logical {
	switch b := other.(type) {
	case *composite:
		switch b.op {
		case Land:
			// A ∪ (B ∩ C)
			// A ∪ (A ∩ B) => A
			for _, it := range b.ch {
				if a.equalsWith(it) {
					return a
				}
			}

			return &composite{op: Lor, ch: []Logical{a, b}}
		case Lor:
			// A ∪ (B ∪ C)
			if b.contains(a) {
				return b
			}

			ret := &composite{op: Lor}
			ret.ch = append(ret.ch, a)
			ret.ch = append(ret.ch, b.ch...)
			return ret
		}
	case *atom:
		if a.equalsWith(b) {
			return a
		}
		reverse := strings.Compare(misc.FirstNonEmptyString(a.sk, a.id), misc.MustFirstNonEmptyString(b.sk, b.id)) == -1
		ret := &composite{op: Lor}
		if reverse {
			ret.ch = []Logical{b, a}
		} else {
			ret.ch = []Logical{a, b}
		}
		return ret
	}

	panic("unreachable")
}

type composite struct {
	op Op
	ch []Logical
}

func (c *composite) phantom() {
}

func (c *composite) Not() Logical {
	switch c.op {
	case Land:
		v := c.ch[0].Not()
		for i := 1; i < len(c.ch); i++ {
			v = v.Or(c.ch[i].Not())
		}
		return v
	case Lor:
		v := c.ch[0].Not()
		for i := 1; i < len(c.ch); i++ {
			v = v.And(c.ch[i].Not())
		}
		return v
	default:
		panic("unreachable")
	}
}

func (c *composite) ToString(and, or string) string {
	if len(c.ch) < 1 {
		return ""
	}

	var sb strings.Builder
	sb.WriteByte('(')

	sb.WriteByte(' ')
	sb.WriteString(c.ch[0].ToString(and, or))

	for i := 1; i < len(c.ch); i++ {
		sb.WriteByte(' ')
		switch c.op {
		case Land:
			sb.WriteString(and)
		case Lor:
			sb.WriteString(or)
		default:
			panic("unreachable")
		}
		sb.WriteByte(' ')
		sb.WriteString(c.ch[i].ToString(and, or))
	}

	sb.WriteByte(' ')
	sb.WriteByte(')')

	return sb.String()
}

func (c *composite) String() string {
	return c.ToString(_and, _or)
}

func (c *composite) And(other Logical) Logical {
	ret := c.and(other)
	if it, ok := ret.(*composite); ok {
		it.optimize()
	}
	return ret
}

func (c *composite) and(other Logical) Logical {
	switch d := other.(type) {
	case *atom:
		return d.And(c)
	case *composite:
		switch c.op {
		case Land:
			switch d.op {
			case Land:
				// (A ∩ B ) ∩ (C ∩ D)
				var ret Logical = c
				for _, it := range d.ch {
					ret = ret.And(it)
				}
				return ret
			case Lor:
				// (A ∩ B) ∩ (C ∪ D) => A ∩ B ∩ (C ∪ D)
				// (A ∩ B) ∩ (C ∪ A) => A ∩ B
				for _, it := range c.ch {
					if next, ok := it.(*atom); ok {
						if d.contains(next) {
							return c
						}
					}
				}
				ret := &composite{
					op: Lor,
				}
				for _, it := range d.ch {
					ret.ch = append(ret.ch, c.And(it))
				}
				return ret
			}
		case Lor:
			switch d.op {
			case Land:
				// (A ∪ B) ∩ (C ∩ D)
				ret := &composite{op: Lor}
				for _, next := range c.ch {
					ret.ch = append(ret.ch, next.And(d))
				}
				return ret
			case Lor:
				// (A ∪ B) ∩ (C ∪ D) => (A ∩ C) ∪ (A ∩ D) ∪ (B ∩ C) ∪ (B ∩ D)
				var ret Logical
				for _, i := range c.ch {
					for _, j := range d.ch {
						next := i.And(j)
						if ret == nil {
							ret = next
						} else {
							ret = ret.Or(next)
						}
					}
				}
				return ret
			}
		}
	}
	panic("unreachable")
}

func (c *composite) Or(other Logical) Logical {
	ret := c.or(other)
	if it, ok := ret.(*composite); ok {
		it.optimize()
	}
	return ret
}

func (c *composite) or(other Logical) Logical {
	switch d := other.(type) {
	case *atom:
		return d.Or(c)
	case *composite:
		switch c.op {
		case Land:
			switch d.op {
			case Land:
				// (A ∩ B) ∪ (C ∩ D)
				return &composite{
					op: Lor,
					ch: []Logical{c, d},
				}
			case Lor:
				// (A ∩ B) ∪ (C ∪ D)
				var ret Logical = c
				for _, it := range d.ch {
					ret = ret.Or(it)
				}
				return ret
			}
		case Lor:
			switch d.op {
			case Land:
				// TODO: 再考虑下这里的展开逻辑
				// (A ∪ B) ∪ (C ∩ D)  => A ∪ B ∪ (C ∩ D)
				// (A ∪ B) ∪ (A ∩ D) => A ∪ B
				for _, it := range c.ch {
					if next, ok := it.(*atom); ok && d.contains(next) {
						return c
					}
				}
				ret := &composite{op: Lor}
				ret.ch = append(ret.ch, c.ch...)
				ret.ch = append(ret.ch, d)
				return ret
			case Lor:
				// (A ∪ B) ∪ (C ∪ D)
				var ret Logical = c
				for _, it := range d.ch {
					ret = ret.Or(it)
				}
				return ret
			}
		}
	}
	panic("unreachable")
}

func (c *composite) optimize() int {
	sort.Sort(sortLogicals(c.ch))
	removed := make(map[int]struct{})
	switch c.op {

	case Lor:
		for i := 0; i < len(c.ch); i++ {
			next, ok := c.ch[i].(*atom)
			if ok {
				for j := i; j < len(c.ch); j++ {
					if it, ok := c.ch[j].(*composite); ok {
						if it.contains(next) {
							removed[j] = struct{}{}
						}
					}
				}
			}
		}
	}

	if len(removed) < 1 {
		return 0
	}

	var newborn []Logical
	for i := 0; i < len(c.ch); i++ {
		if _, ok := removed[i]; ok {
			continue
		}
		newborn = append(newborn, c.ch[i])
	}
	c.ch = newborn
	return len(removed)
}

func (c *composite) contains(a *atom) bool {
	for _, it := range c.ch {
		if a.equalsWith(it) {
			return true
		}
	}
	return false
}

type sortLogicals []Logical

func (s sortLogicals) Len() int {
	return len(s)
}

func (s sortLogicals) Less(i, j int) bool {
	switch a := s[i].(type) {
	case *atom:
		switch b := s[j].(type) {
		case *composite:
			return true
		case *atom:
			k1 := misc.FirstNonEmptyString(a.sk, a.id)
			k2 := misc.FirstNonEmptyString(b.sk, b.id)
			if strings.Compare(k1, k2) == -1 {
				return true
			}
		}
	}
	return false
}

func (s sortLogicals) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func EvalBool(l Logical) (bool, error) {
	v, err := Eval(l, func(a, b interface{}) (interface{}, error) {
		return a.(bool) && b.(bool), nil
	}, func(a, b interface{}) (interface{}, error) {
		return a.(bool) || b.(bool), nil
	}, func(i interface{}) interface{} {
		return !i.(bool)
	})

	if err != nil {
		return false, err
	}
	return v.(bool), err
}

func Eval(l Logical, intersection, union func(a, b interface{}) (interface{}, error), not func(interface{}) interface{}) (ret interface{}, err error) {
	switch t := l.(type) {
	case *atom:
		if t.not {
			ret = not(t.value)
		} else {
			ret = t.value
		}
	case *composite:
		ret, err = Eval(t.ch[0], intersection, union, not)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(t.ch); i++ {
			next := t.ch[i]
			v2, e1 := Eval(next, intersection, union, not)
			if e1 != nil {
				return nil, e1
			}
			switch t.op {
			case Land:
				ret, err = intersection(ret, v2)
			case Lor:
				ret, err = union(ret, v2)
			default:
				panic("unreachable")
			}

		}
	default:
		panic("unreachable")
	}
	return
}
