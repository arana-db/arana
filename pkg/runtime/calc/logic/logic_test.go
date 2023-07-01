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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestLogic(t *testing.T) {
	a := Wrap[String]("a")
	b := Wrap[String]("b")
	c := Wrap[String]("c")
	d := Wrap[String]("d")

	type tt[T Item] struct {
		scene  string
		input  func() Logic[T]
		expect string
	}

	for _, next := range []tt[String]{
		// --- NOT ---
		{
			"!!a",
			func() Logic[String] {
				return NOT(NOT(a))
			},
			"a",
		},
		{
			"!a",
			func() Logic[String] {
				return NOT(a)
			},
			"!a",
		},
		{
			"!(a && b)",
			func() Logic[String] {
				return NOT(AND(a, b))
			},
			"!a || !b",
		},
		{
			"!(b || a)",
			func() Logic[String] {
				return NOT(OR(b, a))
			},
			"!a && !b",
		},
		{
			"!(b && !a)",
			func() Logic[String] {
				return NOT(AND(b, NOT(a)))
			},
			"a || !b",
		},
		// --- AND ---
		{
			"a && b",
			func() Logic[String] {
				return AND(a, b)
			},
			"a && b",
		},
		// --- AND ---
		{
			"a && (a || c)", func() Logic[String] {
				return AND(a, OR(a, c))
			},
			"a",
		},
		{
			"(a || c) && a", func() Logic[String] {
				return AND(OR(a, c), a)
			},
			"a",
		},
		{
			"a && !a", func() Logic[String] {
				return AND(a, NOT(a))
			},
			NONE,
		},
		{
			"!a && a",
			func() Logic[String] {
				return AND(NOT(a), a)
			},
			NONE,
		},
		{
			"a && (b && !a)",
			func() Logic[String] {
				return AND(a, AND(b, NOT(a)))
			},
			NONE,
		},
		{
			"b && (a && c)",
			func() Logic[String] {
				return AND(b, AND(a, c))
			},
			"a && b && c",
		},
		{
			"(a && c) && b",
			func() Logic[String] {
				return AND(AND(a, c), b)
			},
			"a && b && c",
		},
		{
			"!a && (a && b)",
			func() Logic[String] {
				return AND(NOT(a), AND(a, b))
			},
			NONE,
		},
		{
			"(a && b) && (c && !b)",
			func() Logic[String] {
				return AND(AND(a, b), AND(c, NOT(b)))
			},
			NONE,
		},
		{
			"(d && c) && (b && a)",
			func() Logic[String] {
				return AND(AND(d, c), AND(b, a))
			},
			"a && b && c && d",
		},

		// --- OR ---
		{
			"a || b",
			func() Logic[String] {
				return OR(a, b)
			},
			"a || b",
		},
		{
			"b || a",
			func() Logic[String] {
				return OR(b, a)
			},
			"a || b",
		},
		{
			"a || (b || c)",
			func() Logic[String] {
				return OR(a, OR(b, c))
			},
			"a || b || c",
		},
		{
			"(a || c) || b",
			func() Logic[String] {
				return OR(OR(a, c), b)
			},
			"a || b || c",
		},
		{
			"(a || c) || !a",
			func() Logic[String] {
				return OR(OR(a, c), NOT(a))
			},
			"c",
		},
		{
			"(d || c) || (b || a)",
			func() Logic[String] {
				return OR(OR(d, c), OR(b, a))
			},
			"a || b || c || d",
		},
		{
			"(a || b) && (c || a)",
			func() Logic[String] {
				return AND(OR(a, b), OR(c, a))
			},
			"a || (b && c)",
		},
		{
			"(a || b) && (a || b)",
			func() Logic[String] {
				return AND(OR(a, b), OR(a, b))
			},
			"a || b",
		},
		{
			"(a && b) && (c || a)",
			func() Logic[String] {
				return AND(AND(a, b), OR(c, a))
			},
			"a && b",
		},
		{
			"(a && c) || (a || d)",
			func() Logic[String] {
				return OR(AND(a, c), OR(a, d))
			},
			"a || d",
		},
		{
			"(a && b) || (c || d)",
			func() Logic[String] {
				return OR(AND(a, b), OR(c, d))
			},
			"c || d || (a && b)",
		},
		{
			"(a || b) && (c || d)",
			func() Logic[String] {
				return AND(OR(a, b), OR(c, d))
			},
			"(a && c) || (a && d) || (b && c) || (b && d)",
		},
	} {
		t.Run(next.scene, func(t *testing.T) {
			v := next.input()
			assert.Equal(t, next.expect, v.String())
		})
	}
}
