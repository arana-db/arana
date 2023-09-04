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

package runes

import (
	"reflect"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestConvertToRune(t *testing.T) {
	tests := []struct {
		input  interface{}
		output []rune
	}{
		{123, []rune("123")},
		{"hello", []rune("hello")},
		{true, []rune("true")},
		{nil, []rune("<nil>")},
		{struct{ name string }{name: "John"}, []rune("{John}")},
	}

	for _, test := range tests {
		result := ConvertToRune(test.input)
		assert.True(t, reflect.DeepEqual(result, test.output))
	}
}
