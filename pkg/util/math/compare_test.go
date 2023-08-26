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

import "testing"

func TestMax(t *testing.T) {
	// Test case 1：a > b
	result := Max(5, 3)
	expected := 5
	if result != expected {
		t.Errorf("Max(5, 3) returned：%d, expected：%d", result, expected)
	}

	// Test case 2：a < b
	// result = Max(2.5, 6.7)
	// expected = 6.7
	// if result != expected {
	//   t.Errorf("Max(2.5, 6.7) returned：%f, expected：%f", result, expected)
	// }

	// Test case 3：a == b
	result = Max(-10, -10)
	expected = -10
	if result != expected {
		t.Errorf("Max(-10, -10) returned：%d, expected：%d", result, expected)
	}
}

func TestMin(t *testing.T) {
	// Test case 1: a is smaller than b
	result := Min(2, 5)
	expected := 2
	if result != expected {
		t.Errorf("Min(2, 5) returned %d, expected %d", result, expected)
	}

	// Test case 2: b is smaller than a
	result = Min(7, 3)
	expected = 3
	if result != expected {
		t.Errorf("Min(7, 3) returned %d, expected %d", result, expected)
	}

	// Test case 3: a and b are equal
	result = Min(4, 4)
	expected = 4
	if result != expected {
		t.Errorf("Min(4, 4) returned %d, expected %d", result, expected)
	}

	// Test case 4: a is a floating-point number
	resultFloat := Min(2.5, 1.5)
	expectedFloat := 1.5
	if resultFloat != expectedFloat {
		t.Errorf("Min(2.5, 1.5) returned %f, expected %f", resultFloat, expectedFloat)
	}
}
