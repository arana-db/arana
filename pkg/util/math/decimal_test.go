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
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/stretchr/testify/assert"
)

func TestToDecimal(t *testing.T) {
	// Test cases for *gxbig.Decimal input
	decimalInput, _ := gxbig.NewDecFromString("10.5")
	result := ToDecimal(decimalInput)
	assert.Equal(t, decimalInput, result)

	// Test cases for float64 input
	float64Input := 10.5
	expectedResult, _ := gxbig.NewDecFromString("10.5")
	result = ToDecimal(float64Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for float32 input
	float32Input := float32(10.5)
	expectedResult, _ = gxbig.NewDecFromString("10.5")
	result = ToDecimal(float32Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for int input
	intInput := 10
	expectedResult = gxbig.NewDecFromInt(int64(10))
	result = ToDecimal(intInput)
	assert.Equal(t, expectedResult, result)

	// Test cases for int64 input
	int64Input := int64(10)
	expectedResult = gxbig.NewDecFromInt(int64(10))
	result = ToDecimal(int64Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for int32 input
	int32Input := int32(10)
	expectedResult = gxbig.NewDecFromInt(int64(10))
	result = ToDecimal(int32Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for int16 input
	int16Input := int16(10)
	expectedResult = gxbig.NewDecFromInt(int64(10))
	result = ToDecimal(int16Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for int8 input
	int8Input := int8(10)
	expectedResult = gxbig.NewDecFromInt(int64(10))
	result = ToDecimal(int8Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for uint8 input
	uint8Input := uint8(10)
	expectedResult = gxbig.NewDecFromUint(uint64(10))
	result = ToDecimal(uint8Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for uint16 input
	uint16Input := uint16(10)
	expectedResult = gxbig.NewDecFromUint(uint64(10))
	result = ToDecimal(uint16Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for uint32 input
	uint32Input := uint32(10)
	expectedResult = gxbig.NewDecFromUint(uint64(10))
	result = ToDecimal(uint32Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for uint64 input
	uint64Input := uint64(10)
	expectedResult = gxbig.NewDecFromUint(uint64(10))
	result = ToDecimal(uint64Input)
	assert.Equal(t, expectedResult, result)

	// Test cases for uint input
	uintInput := uint(10)
	expectedResult = gxbig.NewDecFromUint(uint64(10))
	result = ToDecimal(uintInput)
	assert.Equal(t, expectedResult, result)

	// Test case for unknown input type
	unknownInput := "invalid"
	expectedResult, _ = gxbig.NewDecFromString("0.0")
	result = ToDecimal(unknownInput)
	assert.Equal(t, expectedResult, result)
}

func TestIsZero(t *testing.T) {
	// Test case 1: d is nil, should return false
	assert.False(t, IsZero(nil))

	// Test case 2: d is not nil and is zero, should return true
	d, _ := gxbig.NewDecFromString("0")
	assert.True(t, IsZero(d))

	// Test case 3: d is not nil and not zero, should return false
	f, _ := gxbig.NewDecFromString("10")
	assert.False(t, IsZero(f))
}
