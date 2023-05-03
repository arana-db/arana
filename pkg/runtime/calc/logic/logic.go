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

package logic

import (
	stdErrors "errors"
	"fmt"
	"strings"
)

import (
	"github.com/google/btree"
)

const (
	NONE = "<NONE>"
	ANY  = "<ANY>"
)

var (
	SymbolAND = "&&"
	SymbolOR  = "||"
	SymbolNOT = "!"
)

var (
	ErrEmptyUnion        = stdErrors.New("no union items found")
	ErrEmptyIntersection = stdErrors.New("no intersection items found")
)

type Item interface {
	Compare(Item) int
}

// Logic represents a logical target which compile to (.. ∩ .. ∩ ..) ∪ (.. ∩ .. ∩ ..) ∪ (.. ∩ .. ∩ ..) ...
// It follows the rules below:
//     1. A ∩ !A <=> 0
//     2. A ∩ B <=> B ∩ A
//     3. A ∪ B <=> B ∪ A
//     4. (A ∩ B) ∩ C <=> A ∩ (B ∩ C)
//     5. (A ∪ B) ∪ C <=> A ∪ (B ∪ C)
//     7. A ∪ (B ∩ C) <=> (A ∪ B) ∩ (A ∪ C)
//     8. A ∩ (B ∪ C) <=> (A ∩ B) ∪ (A ∩ C)
//     9. A ∪ (A ∩ B) <=> A
//     10. A ∩ (A ∪ B) <=> A
//     11. !(A ∩ B) <=> !A ∪ !B
//     12. !(A ∪ B) <=> !A ∩ !B
//     13. A ∪ !A <=> 1
type Logic[T Item] interface {
	fmt.Stringer
	Eval(o Operator[T]) (T, error)
	mustNotImplement()
}

type Operator[T Item] interface {
	AND(first T, others ...T) (T, error)
	OR(first T, others ...T) (T, error)
	NOT(input T) (T, error)
}

type notLogic[T Item] struct {
	origin atomLogic[T]
}

func (n notLogic[T]) String() string {
	var sb strings.Builder
	sb.WriteString(SymbolNOT)
	sb.WriteString(n.origin.String())

	return sb.String()
}

func (n notLogic[T]) Eval(o Operator[T]) (T, error) {
	v, err := n.origin.Eval(o)
	if err != nil {
		return v, err
	}
	return o.NOT(v)
}

func (n notLogic[T]) Value() T {
	return n.origin.Value()
}

func (n notLogic[T]) mustNotImplement() {
}

type atomLogic[T Item] struct {
	t T
}

func (al atomLogic[T]) Eval(o Operator[T]) (T, error) {
	return al.t, nil
}

func (al atomLogic[T]) String() string {
	return fmt.Sprint(al.Value())
}

func (al atomLogic[T]) Value() T {
	return al.t
}

func (al atomLogic[T]) mustNotImplement() {
}

type intersectionLogic[T Item] []Logic[T]

func (in intersectionLogic[T]) Eval(o Operator[T]) (T, error) {
	if len(in) < 1 {
		var t T
		return t, ErrEmptyIntersection
	}
	var values []T
	for i := range in {
		next, err := in[i].Eval(o)
		if err != nil {
			return next, err
		}
		values = append(values, next)
	}
	return o.AND(values[0], values[1:]...)
}

func (in intersectionLogic[T]) String() string {
	if len(in) < 1 {
		return NONE
	}
	var sb strings.Builder

	writeNext := func(l Logic[T]) {
		switch v := l.(type) {
		case atomLogic[T], notLogic[T], intersectionLogic[T]:
			sb.WriteString(v.String())
		case unionLogic[T]:
			sb.WriteByte('(')
			sb.WriteString(v.String())
			sb.WriteByte(')')
		}
	}

	writeNext(in[0])

	for i := 1; i < len(in); i++ {
		sb.WriteByte(' ')
		sb.WriteString(SymbolAND)
		sb.WriteByte(' ')
		writeNext(in[i])
	}

	return sb.String()
}

func (in intersectionLogic[T]) mustNotImplement() {
}

type unionLogic[T Item] []Logic[T]

func (un unionLogic[T]) String() string {
	if len(un) < 1 {
		return "<ALL>"
	}

	var sb strings.Builder

	writeNext := func(l Logic[T]) {
		switch v := l.(type) {
		case atomLogic[T], notLogic[T], unionLogic[T]:
			sb.WriteString(v.String())
		case intersectionLogic[T]:
			sb.WriteByte('(')
			sb.WriteString(v.String())
			sb.WriteByte(')')
		}
	}

	writeNext(un[0])
	for i := 1; i < len(un); i++ {
		sb.WriteByte(' ')
		sb.WriteString(SymbolOR)
		sb.WriteByte(' ')
		writeNext(un[i])
	}
	return sb.String()
}

func (un unionLogic[T]) Eval(o Operator[T]) (T, error) {
	if len(un) < 1 {
		var t T
		return t, ErrEmptyUnion
	}
	var values []T
	for i := range un {
		next, err := un[i].Eval(o)
		if err != nil {
			return next, err
		}
		values = append(values, next)
	}
	return o.OR(values[0], values[1:]...)
}

func (un unionLogic[T]) mustNotImplement() {
}

func AND[T Item](first, second Logic[T]) Logic[T] {
	switch a := first.(type) {
	case atomLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomAndAtom[T](a, b)
		case notLogic[T]:
			return atomAndNot[T](a, b)
		case intersectionLogic[T]:
			return atomAndIntersection[T](a, b)
		case unionLogic[T]:
			return atomAndUnion[T](a, b)
		}
	case notLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomAndNot[T](b, a)
		case notLogic[T]:
			return notAndNot[T](a, b)
		case intersectionLogic[T]:
			return notAndIntersection[T](a, b)
		case unionLogic[T]:
			return notAndUnion[T](a, b)
		}
	case intersectionLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomAndIntersection[T](b, a)
		case notLogic[T]:
			return notAndIntersection[T](b, a)
		case intersectionLogic[T]:
			return intersectionAndIntersection[T](a, b)
		case unionLogic[T]:
			return intersectionAndUnion[T](a, b)
		}
	case unionLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return AND[T](b, a)
		case notLogic[T]:
			return notAndUnion[T](b, a)
		case intersectionLogic[T]:
			return intersectionAndUnion[T](b, a)
		case unionLogic[T]:
			return unionAndUnion[T](a, b)
		}
	}
	panic("unreachable")
}

func Wrap[T Item](v T) Logic[T] {
	return atomLogic[T]{t: v}
}

func NOT[T Item](input Logic[T]) Logic[T] {
	switch it := input.(type) {
	case atomLogic[T]:
		return notLogic[T]{origin: it}
	case notLogic[T]:
		return it.origin
	case intersectionLogic[T]:
		ret := make([]Logic[T], 0, len(it))
		for i := range it {
			ret = append(ret, NOT(it[i]))
		}
		return unionLogic[T](ret)
	case unionLogic[T]:
		ret := make([]Logic[T], 0, len(it))
		for i := range it {
			ret = append(ret, NOT(it[i]))
		}
		return intersectionLogic[T](ret)
	}
	panic("unreachable")
}

func True[T Item]() Logic[T] {
	return (unionLogic[T])(nil)
}

func False[T Item]() Logic[T] {
	return (intersectionLogic[T])(nil)
}

func OR[T Item](first, second Logic[T]) Logic[T] {
	switch a := first.(type) {
	case atomLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomOrAtom[T](a, b)
		case notLogic[T]:
			return atomOrNot[T](a, b)
		case intersectionLogic[T]:
			return atomOrIntersection[T](a, b)
		case unionLogic[T]:
			return atomOrUnion[T](a, b)
		}
	case notLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomOrNot[T](b, a)
		case notLogic[T]:
			return notOrNot[T](a, b)
		case intersectionLogic[T]:
			return notOrIntersection[T](a, b)
		case unionLogic[T]:
			return notOrUnion[T](a, b)
		}
	case intersectionLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomOrIntersection[T](b, a)
		case notLogic[T]:
			return notOrIntersection[T](b, a)
		case intersectionLogic[T]:
			return intersectionOrIntersection[T](a, b)
		case unionLogic[T]:
			return intersectionOrUnion(a, b)
		}
	case unionLogic[T]:
		switch b := second.(type) {
		case atomLogic[T]:
			return atomOrUnion[T](b, a)
		case notLogic[T]:
			return notOrUnion[T](b, a)
		case intersectionLogic[T]:
			return intersectionOrUnion(b, a)
		case unionLogic[T]:
			return unionOrUnion[T](a, b)
		}
	}

	panic("unreachable")
}

func atomAndNot[T Item](a atomLogic[T], b notLogic[T]) Logic[T] {
	switch a.Value().Compare(b.Value()) {
	case 0:
		// A ∩ !A => NaN
		return (intersectionLogic[T])(nil)
	case -1:
		return intersectionLogic[T]{a, b}
	case 1:
		return intersectionLogic[T]{b, a}
	}
	panic("unreachable")
}

func notAndNot[T Item](a, b notLogic[T]) Logic[T] {
	switch a.Value().Compare(b.Value()) {
	case 1:
		// !B ∩ !A => !A ∩ !B
		return intersectionLogic[T]{b, a}
	case -1:
		// !A ∩ !B => !A ∩ !B
		return intersectionLogic[T]{a, b}
	case 0:
		// !A ∩ !A => !A
		return a
	}
	panic("unreachable")
}

func atomAndAtom[T Item](a, b atomLogic[T]) Logic[T] {
	switch a.Value().Compare(b.Value()) {
	case 1:
		// B ∩ A => A ∩ B
		return intersectionLogic[T]{b, a}
	case -1:
		// A ∩ B => A ∩ B
		return intersectionLogic[T]{a, b}
	case 0:
		// A ∩ A => A
		return a
	}
	panic("unreachable")
}

func notOrNot[T Item](a, b notLogic[T]) Logic[T] {
	switch a.Value().Compare(b.Value()) {
	case 0:
		return a
	case -1:
		return unionLogic[T]{a, b}
	case 1:
		return unionLogic[T]{b, a}
	}
	panic("unreachable")
}

func atomOrNot[T Item](a atomLogic[T], b notLogic[T]) Logic[T] {
	switch a.Value().Compare(b.Value()) {
	case 0:
		return unionLogic[T](nil)
	case -1:
		return unionLogic[T]{a, b}
	case 1:
		return unionLogic[T]{b, a}
	}
	panic("unreachable")
}

func atomOrAtom[T Item](a, b atomLogic[T]) Logic[T] {
	switch a.Value().Compare(b.Value()) {
	case 0:
		return a
	case -1:
		return unionLogic[T]{a, b}
	case 1:
		return unionLogic[T]{b, a}
	}
	panic("unreachable")
}

func atomAndIntersection[T Item](a atomLogic[T], b intersectionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return (intersectionLogic[T])(nil)
	}
	// A ∩ ( A ∩ B ∩ C ) => A ∩ B ∩ C
	ret := make([]Logic[T], 0, len(b)+1)
	var added bool
	for i := range b {
		if added {
			ret = append(ret, b[i])
			continue
		}

		switch it := b[i].(type) {
		case atomLogic[T]:
			switch a.Value().Compare(it.Value()) {
			case 0:
				ret = append(ret, b[i])
				added = true
				continue
			case -1:
				ret = append(ret, a, b[i])
				added = true
				continue
			}
		case notLogic[T]:
			switch a.Value().Compare(it.Value()) {
			case 0:
				return (intersectionLogic[T])(nil)
			case -1:
				ret = append(ret, a, b[i])
				added = true
				continue
			}
		}

		ret = append(ret, b[i])
	}
	if !added {
		ret = append(ret, a)
	}
	return intersectionLogic[T](ret)
}

func atomAndUnion[T Item](a atomLogic[T], b unionLogic[T]) Logic[T] {
	// A ∩ (B ∪ C) => (A ∩ B) ∪ (A ∩ C)
	// A ∩ (A ∪ B) => A
	for i := range b {
		switch next := b[i].(type) {
		case atomLogic[T]:
			if next.Value().Compare(a.Value()) == 0 {
				return a
			}
		}
	}
	var ret []Logic[T]
	for i := range b {
		ret = append(ret, AND[T](a, b[i]))
	}
	return unionLogic[T](ret)
}

func notAndUnion[T Item](a notLogic[T], b unionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return a
	}

	// !A ∩ (A ∪ B) => (!A ∩ A) ∪ (!A ∩ B) => !A ∩ B
	var ret []Logic[T]
	for i := range b {
		next := AND[T](a, b[i])
		if next == nil {
			continue
		}
		switch v := next.(type) {
		case unionLogic[T]:
			if len(v) < 1 {
				return (unionLogic[T])(nil)
			}
		case intersectionLogic[T]:
			if len(v) < 1 {
				continue
			}
		}
		ret = append(ret, next)
	}

	if len(ret) < 1 {
		return (intersectionLogic[T])(nil)
	}

	return unionLogic[T](ret)
}

func notAndIntersection[T Item](a notLogic[T], b intersectionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return nil
	}

	// !B ∩ (A ∩ C) => A ∩ !B ∩ C
	// !B ∩ (B ∩ C) => NaN
	ret := make([]Logic[T], 0, len(b)+1)

	for i := range b {
		next := AND[T](a, b[i])

		switch v := next.(type) {
		case intersectionLogic[T]:
			if len(v) < 1 {
				return (intersectionLogic[T])(nil)
			}
		case unionLogic[T]:
			if len(v) < 1 {
				continue
			}
		}

		ret = append(ret, next)
	}

	return (intersectionLogic[T])(ret)
}

func atomOrIntersection[T Item](a atomLogic[T], b intersectionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return a
	}
	// A ∪ (B ∩ C)
	// A ∪ (A ∩ B) => A
	for i := range b {
		switch it := b[i].(type) {
		case atomLogic[T]:
			if a.Value().Compare(it.Value()) == 0 {
				return a
			}
		}
	}
	return unionLogic[T]{a, b}
}

func notOrIntersection[T Item](a notLogic[T], b intersectionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return a
	}
	// A ∪ (B ∩ C)
	// A ∪ (A ∩ B) => A
	for i := range b {
		switch it := b[i].(type) {
		case notLogic[T]:
			if a.Value().Compare(it.Value()) == 0 {
				return a
			}
		}
	}
	return unionLogic[T]{a, b}
}

func atomOrUnion[T Item](a atomLogic[T], b unionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return b
	}

	ret := make([]Logic[T], 0, len(b)+1)
	var added bool
	for i := range b {
		if added {
			ret = append(ret, b[i])
			continue
		}
		switch next := b[i].(type) {
		case atomLogic[T]:
			switch a.Value().Compare(next.Value()) {
			case -1:
				ret = append(ret, a, next)
				added = true
			case 0: // A ∪ (A ∪ B) => A ∪ B
				added = true
				ret = append(ret, a)
			case 1:
				ret = append(ret, next)
			}
			continue
		case notLogic[T]:
			switch a.Value().Compare(next.Value()) {
			case -1:
				ret = append(ret, a, next)
				added = true
			case 0: // A ∪ (!A ∪ B) => B
				added = true
			case 1:
				ret = append(ret, next)
			}
			continue
		default:
			ret = append(ret, next)
		}
	}

	if !added {
		ret = append(ret, a)
	}

	if len(ret) < 1 {
		return unionLogic[T](nil)
	}

	return unionLogic[T](ret)
}

func notOrUnion[T Item](a notLogic[T], b unionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return b
	}

	// !A ∪ (!A ∪ B) => !A ∪ B
	ret := make([]Logic[T], 0, len(b)+1)
	var added bool

	for i := range b {
		if added {
			ret = append(ret, b[i])
			continue
		}
		switch next := b[i].(type) {
		case notLogic[T]:
			switch a.Value().Compare(next.Value()) {
			case -1:
				added = true
				ret = append(ret, a, next)
			case 1:
				ret = append(ret, next)
			case 0:
				added = true
				ret = append(ret, a)
			}
			continue
		case atomLogic[T]:
			switch a.Value().Compare(next.Value()) {
			case -1:
				added = true
				ret = append(ret, a, next)
			case 1:
				ret = append(ret, next)
			case 0:
				added = true
			}
			continue
		default:
			ret = append(ret, next)
		}
	}

	if len(ret) < 1 {
		return unionLogic[T](nil)
	}

	return unionLogic[T](ret)
}

func intersectionOrUnion[T Item](a intersectionLogic[T], b unionLogic[T]) Logic[T] {
	if len(b) < 1 {
		return unionLogic[T](nil)
	}
	if len(a) < 1 {
		return b
	}

	// (C ∩ D) ∪ (A ∪ B) => A ∪ B ∪ (C ∩ D)
	// (A ∩ D) ∪ (A ∪ B) =>  A ∪ B

	for i := range a {
		for j := range b {
			switch x := a[i].(type) {
			case atomLogic[T]:
				switch y := b[j].(type) {
				case atomLogic[T]:
					if x.Value().Compare(y.Value()) == 0 {
						return b
					}
				}
			case notLogic[T]:
				switch y := b[j].(type) {
				case notLogic[T]:
					if x.Value().Compare(y.Value()) == 0 {
						return b
					}
				}
			}
		}
	}

	bt := btree.NewG[Logic[T]](2, Less[T])

	bt.ReplaceOrInsert(a)
	for i := range b {
		bt.ReplaceOrInsert(b[i])
	}

	ret := make([]Logic[T], 0, bt.Len())
	bt.Ascend(func(item Logic[T]) bool {
		ret = append(ret, item)
		return true
	})

	return (unionLogic[T])(ret)
}

func intersectionOrIntersection[T Item](a, b intersectionLogic[T]) Logic[T] {
	if len(a) < 1 {
		return b
	}
	if len(b) < 1 {
		return a
	}

	// (A ∩ B) ∪ (C ∩ D)
	return unionLogic[T]{a, b}
}

func unionAndUnion[T Item](a, b unionLogic[T]) Logic[T] {
	if len(a) < 1 {
		return b
	}
	if len(b) < 1 {
		return a
	}

	// (A ∪ B) ∩ (C ∪ D) => (A ∩ C) ∪ (A ∩ D) ∪ (B ∩ C) ∪ (B ∩ D)

	var result Logic[T]
	for i := range a {
		for j := range b {
			next := AND(a[i], b[j])
			if result == nil {
				result = next
			} else {
				result = OR(result, next)
			}
		}
	}

	return result
}

func unionOrUnion[T Item](a, b unionLogic[T]) Logic[T] {
	if len(a) < 1 || len(b) < 1 {
		return (unionLogic[T])(nil)
	}

	// (A ∪ B) ∪ (C ∪ D) => A ∪ B ∪ C ∪ D

	bt := btree.NewG[Logic[T]](2, Less[T])
	for i := range b {
		switch next := OR[T](a, b[i]).(type) {
		case atomLogic[T], notLogic[T], intersectionLogic[T]:
			bt.ReplaceOrInsert(next)
		case unionLogic[T]:
			if len(next) < 1 {
				return unionLogic[T](nil)
			}
			for j := range next {
				bt.ReplaceOrInsert(next[j])
			}
		}
	}

	var ret unionLogic[T]
	bt.Ascend(func(item Logic[T]) bool {
		ret = append(ret, item)
		return true
	})
	return ret
}

func intersectionAndIntersection[T Item](a, b intersectionLogic[T]) Logic[T] {
	if len(a) < 1 || len(b) < 1 {
		return intersectionLogic[T](nil)
	}

	bt := btree.NewG[Logic[T]](2, Less[T])

	for i := range a {
		next := AND[T](a[i], b)
		switch val := next.(type) {
		case atomLogic[T], notLogic[T]:
			bt.ReplaceOrInsert(next)
		case intersectionLogic[T]:
			if len(val) < 1 {
				return intersectionLogic[T](nil)
			}
			for j := range val {
				bt.ReplaceOrInsert(val[j])
			}
		case unionLogic[T]:
			panic("unreachable")
		}
	}

	var ret intersectionLogic[T]
	bt.Ascend(func(item Logic[T]) bool {
		ret = append(ret, item)
		return true
	})

	return ret
}

func intersectionAndUnion[T Item](a intersectionLogic[T], b unionLogic[T]) Logic[T] {
	if len(a) < 1 || len(b) < 1 {
		return a
	}
	// (A ∩ B) ∩ (C ∪ A) => A ∩ B
	for i := range a {
		for j := range b {
			switch x := a[i].(type) {
			case atomLogic[T]:
				switch y := b[j].(type) {
				case atomLogic[T]:
					if x.Value().Compare(y.Value()) == 0 {
						return a
					}
				}
			case notLogic[T]:
				switch y := b[j].(type) {
				case notLogic[T]:
					if x.Value().Compare(y.Value()) == 0 {
						return a
					}
				}
			}
		}
	}

	// (A ∩ B) ∩ (C ∪ D) => (A ∩ B ∩ C) ∪ (A ∩ B ∩ D)
	bt := btree.NewG[Logic[T]](2, Less[T])
	for i := range b {
		next := AND[T](a, b[i])
		bt.ReplaceOrInsert(next)
	}

	var ret unionLogic[T]
	bt.Ascend(func(item Logic[T]) bool {
		ret = append(ret, item)
		return true
	})

	return ret
}

func Compare[T Item](prev, next Logic[T]) int {
	switch a := prev.(type) {
	case atomLogic[T]:
		switch b := next.(type) {
		case atomLogic[T]:
			return a.Value().Compare(b.Value())
		case notLogic[T]:
			return a.Value().Compare(b.Value())
		default:
			return -1
		}
	case notLogic[T]:
		switch b := next.(type) {
		case atomLogic[T]:
			switch a.Value().Compare(b.Value()) {
			case -1, 0:
				return -1
			default:
				return 1
			}
		case notLogic[T]:
			return a.Value().Compare(b.Value())
		default:
			return -1
		}
	case intersectionLogic[T]:
		switch b := next.(type) {
		case intersectionLogic[T]:
			if len(a) < len(b) {
				return -1
			}
			if len(a) > len(b) {
				return 1
			}
			for i := 0; i < len(a); i++ {
				c := Compare(a[i], b[i])
				switch c {
				case 1, -1:
					return c
				}
			}
			return 0
		case unionLogic[T]:
			if len(a) < len(b) {
				return -1
			}
			if len(a) > len(b) {
				return 1
			}
			for i := 0; i < len(a); i++ {
				c := Compare(a[i], b[i])
				switch c {
				case 1, -1:
					return c
				}
			}
			return 1
		}
	case unionLogic[T]:
		switch b := next.(type) {
		case intersectionLogic[T]:
			if len(a) < len(b) {
				return -1
			}
			if len(a) > len(b) {
				return 1
			}
			for i := 0; i < len(a); i++ {
				c := Compare(a[i], b[i])
				switch c {
				case 1, -1:
					return c
				}
			}
			return -1
		case unionLogic[T]:
			if len(a) < len(b) {
				return -1
			}
			if len(a) > len(b) {
				return 1
			}
			for i := 0; i < len(a); i++ {
				c := Compare(a[i], b[i])
				switch c {
				case 1, -1:
					return c
				}
			}
			return 0
		}
	}
	panic("unreachable")
}

func Less[T Item](prev, next Logic[T]) bool {
	return Compare(prev, next) == -1
}
