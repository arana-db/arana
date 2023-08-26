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

func TestAbs(t *testing.T) {
	// Test cases for positive numbers
	if result := Abs(5); result != 5 {
		t.Errorf("Abs(5) = %v; want 5", result)
	}

	if result := Abs(int32(10)); result != int32(10) {
		t.Errorf("Abs(int32(10)) = %v; want 10", result)
	}

	// Test cases for negative numbers
	if result := Abs(-5); result != 5 {
		t.Errorf("Abs(-5) = %v; want 5", result)
	}

	if result := Abs(int64(-10)); result != int64(10) {
		t.Errorf("Abs(int64(-10)) = %v; want 10", result)
	}

	// Test case for zero
	if result := Abs(0); result != 0 {
		t.Errorf("Abs(0) = %v; want 0", result)
	}
}
