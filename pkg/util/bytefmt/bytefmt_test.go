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

package bytefmt

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestByteSize(t *testing.T) {
	tables := []struct {
		in   uint64
		want string
	}{
		{in: ^uint64(0), want: "16E"},
		{in: 10 * EXABYTE, want: "10E"},
		{in: 10.5 * EXABYTE, want: "10.5E"},
		{in: 10 * PETABYTE, want: "10P"},
		{in: 10.5 * PETABYTE, want: "10.5P"},
		{in: 10 * TERABYTE, want: "10T"},
		{in: 10.5 * TERABYTE, want: "10.5T"},
		{in: 10 * GIGABYTE, want: "10G"},
		{in: 10.5 * GIGABYTE, want: "10.5G"},
		{in: 10 * MEGABYTE, want: "10M"},
		{in: 10.5 * MEGABYTE, want: "10.5M"},
		{in: 10 * KILOBYTE, want: "10K"},
		{in: 10.5 * KILOBYTE, want: "10.5K"},
		{in: 268435456, want: "256M"},
		{in: 0, want: "0"},
		{in: 1000, want: "1000B"},
		{in: 1024, want: "1K"},
	}
	for i := 0; i < len(tables); i++ {
		assert.Equal(t, tables[i].want, ByteSize(tables[i].in))
	}
}

func TestToBytes(t *testing.T) {
	tables := []struct {
		in   string
		want uint64
	}{
		{in: "10B", want: 10},
		{in: "4.5KB", want: 4608},
		{in: "13.5KB", want: 13824},
		{in: "5MB", want: 5 * MEGABYTE},
		{in: "5mb", want: 5 * MEGABYTE},
		{in: "256M", want: 268435456},
		{in: "2GB", want: 2 * GIGABYTE},
		{in: "3TB", want: 3 * TERABYTE},
		{in: "3PB", want: 3 * PETABYTE},
		{in: "3EB", want: 3 * EXABYTE},
	}
	t.Log(0x120a)
	for i := 0; i < len(tables); i++ {
		byteSize, err := ToBytes(tables[i].in)
		assert.NoError(t, err)
		assert.Equal(t, tables[i].want, byteSize)
	}
}

func TestToBytes_Error(t *testing.T) {
	testCases := []struct {
		input          string
		expectedResult uint64
		expectedError  error
	}{
		// Invalid input cases
		{"", 0, errInvalidByteQuantity},
		{"   ", 0, errInvalidByteQuantity},
		{"-1B", 0, errInvalidByteQuantity},
		{"1KB1B", 0, errInvalidByteQuantity},
		{"1MB1KB", 0, errInvalidByteQuantity},
		{"1GB1MB", 0, errInvalidByteQuantity},
		{"1TB1GB", 0, errInvalidByteQuantity},
		{"1PB1TB", 0, errInvalidByteQuantity},
		{"1EB1PB", 0, errInvalidByteQuantity},
		{"1ZB", 0, errInvalidByteQuantity},
		{"1ZIB", 0, errInvalidByteQuantity},
	}

	for _, testCase := range testCases {
		result, err := ToBytes(testCase.input)

		// Assert the result
		if result != testCase.expectedResult {
			t.Errorf("Expected result: %d, but got: %d", testCase.expectedResult, result)
		}

		// Assert the error
		if err != testCase.expectedError {
			t.Errorf("Expected error: %v, but got: %v", testCase.expectedError, err)
		}
	}
}
