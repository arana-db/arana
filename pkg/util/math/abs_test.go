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

func TestAbs(t *testing.T) {
	// Test cases for positive numbers
	assert.Equal(t, 5, Abs(5))
	assert.Equal(t, int32(10), Abs(int32(10)))

	// Test cases for negative numbers
	assert.Equal(t, 5, Abs(-5))
	assert.Equal(t, int64(10), Abs(int64(-10)))

	// Test case for zero
	assert.Equal(t, 0, Abs(0))
}
