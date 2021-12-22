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
	"github.com/dubbogo/arana/pkg/proto/rule"
)

var (
	_ rule.Range = (*singleRange)(nil)
	_ rule.Range = (*sliceRange)(nil)
)

type sliceRange struct {
	cur    int
	values []interface{}
}

func (s *sliceRange) HasNext() bool {
	return s.cur < len(s.values)
}

func (s *sliceRange) Next() interface{} {
	ret := s.values[s.cur]
	s.cur++
	return ret
}

type singleRange struct {
	n     uint8
	value interface{}
}

func (s *singleRange) HasNext() bool {
	return s.n == 0
}

func (s *singleRange) Next() interface{} {
	if s.n == 0 {
		s.n += 1
	}
	return s.value
}

type filterRange struct {
	inner  rule.Range
	filter func(next interface{}) bool
	next   interface{}
}

func (f *filterRange) HasNext() bool {
	if !f.inner.HasNext() {
		f.inner = nil
		return false
	}
	next := f.inner.Next()
	if f.filter(next) {
		f.next = next
		return true
	}
	return f.HasNext()
}

func (f *filterRange) Next() interface{} {
	return f.next
}

// Multiple wraps multiple values as an Iterator.
func Multiple(values ...interface{}) rule.Range {
	return &sliceRange{
		values: values,
	}
}

// Single wraps a single value as an Iterator.
func Single(value interface{}) rule.Range {
	return &singleRange{
		value: value,
	}
}

// Filter wraps an existing Iterator with a filter.
func Filter(src rule.Range, predicate func(interface{}) bool) rule.Range {
	return &filterRange{
		inner:  src,
		filter: predicate,
	}
}
