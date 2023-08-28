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

package math

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	// Test case 1：a > b
	result := Max(5, 3)
	assert.Equal(t, 5, result)

	// Test case 2：a < b
	// result = Max(2.5, 6.7)
	// expected = 6.7
	// if result != expected {
	//   t.Errorf("Max(2.5, 6.7) returned：%f, expected：%f", result, expected)
	// }

	// Test case 3：a == b
	result = Max(-10, -10)
	assert.Equal(t, -10, result)
}

func TestMin(t *testing.T) {
	// Test case 1: a is smaller than b
	result := Min(2, 5)
	assert.Equal(t, 2, result)

	// Test case 2: b is smaller than a
	result = Min(7, 3)
	assert.Equal(t, 3, result)

	// Test case 3: a and b are equal
	result = Min(4, 4)
	assert.Equal(t, 4, result)

	// Test case 4: a is a floating-point number
	resultFloat := Min(2.5, 1.5)
	assert.Equal(t, 1.5, resultFloat)
}
