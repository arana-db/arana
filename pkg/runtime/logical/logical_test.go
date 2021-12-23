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
package logical_test

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	. "github.com/dubbogo/arana/pkg/runtime/logical"
)

func TestLogicalAndOr(t *testing.T) {
	for _, it := range []Logical{
		New("A").And(New("B")).And(New("C")),
		New("A").And(New("B").And(New("C"))),
		New("A").And(New("A")),
		New("A").Or(New("A")),
		New("A").And(New("A").Or(New("B"))),
		New("A").And(New("B")).Or(New("A")),
		New("A").Or(New("B")).And(New("C").Or(New("D"))),
		New("A").And(New("B")).And(New("C").And(New("D"))),
		New("A").Or(New("B")).And(New("C").Or(New("A"))),
		New("A").Or(New("B")).And(New("A").Or(New("B"))),
		New("A").And(New("B")).And(New("C").Or(New("A"))),
	} {
		t.Log(it)
	}
}

func TestNot(t *testing.T) {
	for _, it := range []Logical{
		New("A").And(New("B")).Not(),
	} {
		t.Log(it)
	}
}

func TestEval(t *testing.T) {
	l := New("A", WithValue(true)).And(New("B", WithValue(false)))
	result, err := EvalBool(l)
	assert.NoError(t, err)
	t.Logf("%s = %v\n", l, result)
}
