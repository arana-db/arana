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

package rule

import (
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/util/rand2"
)

const (
	_ StepUnit = iota
	Uhour
	Uday
	Uweek
	Umonth
	Uyear
	Unum
	Ustr
)

var (
	DefaultNumberStepper = Stepper{U: Unum, N: 1}
)

// Range represents a value range.
type Range interface {
	// HasNext returns true if Range is not EOF.
	HasNext() bool
	// Next returns the next value.
	Next() interface{}
}

// StepUnit represents the unit of a Stepper.
type StepUnit int8

// IsTime returns true if the unit is time.
func (u StepUnit) IsTime() bool {
	switch u {
	case Uhour, Uday, Uweek, Umonth, Uyear:
		return true
	}
	return false
}

func (u StepUnit) String() string {
	switch u {
	case Uhour:
		return "HOUR"
	case Uday:
		return "DATE"
	case Uweek:
		return "WEEK"
	case Umonth:
		return "MONTH"
	case Uyear:
		return "YEAR"
	case Unum:
		return "NUMBER"
	case Ustr:
		return "STRING"
	default:
		return "UNKNOWN"
	}
}

// Stepper represents the value stepper in a sharding rule.
type Stepper struct {
	N int      // N is the step value.
	U StepUnit // U is the step unit.
}

func (s Stepper) After(offset interface{}) (interface{}, error) {
	return s.compute(offset, s.N)
}

func (s Stepper) Before(offset interface{}) (interface{}, error) {
	return s.compute(offset, -s.N)
}

func (s Stepper) Ascend(offset interface{}, n int) (Range, error) {
	return s.computeRange(offset, n, false)
}

func (s Stepper) Descend(offset interface{}, n int) (Range, error) {
	return s.computeRange(offset, n, true)
}

func (s Stepper) computeRange(offset interface{}, cnt int, reverse bool) (Range, error) {
	if offset == nil {
		return nil, errors.New("offset is nil")
	}
	switch s.U {
	case Unum:
		switch cur := offset.(type) {
		case int32:
			return s.iterInt32(cur, cnt, reverse), nil
		case int:
			return s.iterInt(cur, cnt, reverse), nil
		case int64:
			return s.iterInt64(cur, cnt, reverse), nil
		}
	case Uhour:
		switch cur := offset.(type) {
		case time.Time:
			return s.iterTime(cur, cnt, time.Hour, reverse), nil
		}
	case Uday:
		switch cur := offset.(type) {
		case time.Time:
			const duDay = 24 * time.Hour
			return s.iterTime(cur, cnt, duDay, reverse), nil
		}
	case Uweek:
		switch cur := offset.(type) {
		case time.Time:
			const duWeek = 7 * 24 * time.Hour
			return s.iterTime(cur, cnt, duWeek, reverse), nil
		}
	case Umonth:
	case Uyear:
	case Ustr:
		return &iterStr{length: 16, cnt: cnt}, nil
	}
	return nil, errors.Errorf("unsupported offset type: type=%T, unit=%s", offset, s.U)
}

func (s Stepper) String() string {
	var sb strings.Builder
	sb.WriteString("Stepper{N=")
	sb.WriteString(strconv.FormatInt(int64(s.N), 10))
	sb.WriteString(",U=")
	sb.WriteString(s.U.String())
	sb.WriteByte('}')
	return sb.String()
}

func (s Stepper) compute(offset interface{}, n int) (interface{}, error) {
	if offset == nil {
		return nil, errors.New("offset is nil")
	}
	switch s.U {
	case Unum:
		switch cur := offset.(type) {
		case int32:
			return cur + int32(n), nil
		case int:
			return cur + n, nil
		case int64:
			return cur + int64(n), nil
		}
	case Uhour:
		switch cur := offset.(type) {
		case time.Time:
			return cur.Add(time.Duration(n) * time.Hour), nil
		}
	case Uday:
		switch cur := offset.(type) {
		case time.Time:
			const duDay = 24 * time.Hour
			return cur.Add(time.Duration(n) * duDay), nil
		}
	case Uweek:
		switch cur := offset.(type) {
		case time.Time:
			const duWeek = 7 * 24 * time.Hour
			return cur.Add(time.Duration(n) * duWeek), nil
		}
	case Umonth:
	case Uyear:
	case Ustr:
		return &iterStr{length: 16, cnt: n}, nil
	}
	return nil, errors.Errorf("unsupported offset type: type=%T, unit=%s", offset, s.U)
}

func (s Stepper) iterTime(offset time.Time, cnt int, timeUnit time.Duration, reverse bool) Range {
	step := time.Duration(s.N) * timeUnit
	if reverse {
		step *= -1
	}

	return &iterTime{
		cur:  offset,
		step: step,
		cnt:  cnt,
	}
}

func (s Stepper) iterInt32(offset int32, cnt int, reverse bool) Range {
	step := int32(s.N)
	if reverse {
		step *= -1
	}
	return &iterInt32{
		cur:  offset,
		step: step,
		cnt:  cnt,
	}
}

func (s Stepper) iterInt(offset int, cnt int, reverse bool) Range {
	step := s.N
	if reverse {
		step *= -1
	}

	return &iterInt{
		cur:  offset,
		step: step,
		cnt:  cnt,
	}
}

func (s Stepper) iterInt64(offset int64, cnt int, reverse bool) Range {
	step := int64(s.N)
	if reverse {
		step *= -1
	}
	return &iterInt64{
		cur:  offset,
		step: step,
		cnt:  cnt,
	}
}

type iterStr struct {
	length int
	cnt    int
}

func (i *iterStr) HasNext() bool {
	return i.cnt > 0
}

func (i *iterStr) Next() interface{} {
	if i.cnt <= 0 {
		panic("iterator is exhausted!")
	}
	i.cnt--

	return strconv.FormatInt(int64(rand2.Int31()), 10)
}

type iterInt struct {
	cur  int
	step int
	cnt  int
}

func (i *iterInt) HasNext() bool {
	return i.cnt > 0
}

func (i *iterInt) Next() interface{} {
	if i.cnt == 0 {
		panic("iterator is exhausted!")
	}
	prev := i.cur
	i.cur += i.step
	i.cnt -= 1
	return prev
}

type iterInt32 struct {
	cur  int32
	step int32
	cnt  int
}

func (i *iterInt32) HasNext() bool {
	return i.cnt > 0
}

func (i *iterInt32) Next() interface{} {
	if i.cnt == 0 {
		panic("iterator is exhausted!")
	}
	prev := i.cur
	i.cur += i.step
	i.cnt -= 1
	return prev
}

type iterInt64 struct {
	cur  int64
	step int64
	cnt  int
}

func (i *iterInt64) HasNext() bool {
	return i.cnt > 0
}

func (i *iterInt64) Next() interface{} {
	if i.cnt == 0 {
		panic("iterator is exhausted!")
	}
	prev := i.cur
	i.cur += i.step
	i.cnt -= 1
	return prev
}

type iterTime struct {
	cur  time.Time
	step time.Duration
	cnt  int
}

func (i *iterTime) HasNext() bool {
	return i.cnt > 0
}

func (i *iterTime) Next() interface{} {
	if i.cnt == 0 {
		panic("iterator is exhausted!")
	}
	prev := i.cur
	i.cur = i.cur.Add(i.step)
	i.cnt -= 1
	return prev
}
