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

package config

import (
	"testing"
)

func TestIsEnableLocalMathCompu(t *testing.T) {
	// Test case 1: Enable local math computation
	_enableLocalMathCompu = false
	got := IsEnableLocalMathCompu(true)
	if got != true {
		t.Errorf("Expected true, got %v", got)
	}

	// Test case 2: Disable local math computation
	_enableLocalMathCompu = true
	got = IsEnableLocalMathCompu(false)
	if got != true {
		t.Errorf("Expected true, got %v", got)
	}

	// Test case 3: Local math computation already enabled
	_enableLocalMathCompu = true
	got = IsEnableLocalMathCompu(true)
	if got != true {
		t.Errorf("Expected true, got %v", got)
	}

	// Test case 4: Local math computation already disabled
	_enableLocalMathCompu = false
	got = IsEnableLocalMathCompu(false)
	if got != false {
		t.Errorf("Expected false, got %v", got)
	}
}
