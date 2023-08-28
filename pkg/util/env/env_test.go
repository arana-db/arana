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

package env

import (
	"os"
	"testing"
)

import (
	"github.com/arana-db/arana/pkg/constants"
)

func TestIsDevelopEnvironment(t *testing.T) {
	// Test case 1: Environment variable is set to "1"
	os.Setenv(constants.EnvDevelopEnvironment, "1")
	isDev := IsDevelopEnvironment()
	if !isDev {
		t.Errorf("Expected IsDevelopEnvironment() to return true, but got false")
	}
	os.Unsetenv(constants.EnvDevelopEnvironment)

	// Test case 2: Environment variable is set to "yes"
	os.Setenv(constants.EnvDevelopEnvironment, "yes")
	isDev = IsDevelopEnvironment()
	if !isDev {
		t.Errorf("Expected IsDevelopEnvironment() to return true, but got false")
	}
	os.Unsetenv(constants.EnvDevelopEnvironment)

	// Test case 3: Environment variable is set to "on"
	os.Setenv(constants.EnvDevelopEnvironment, "on")
	isDev = IsDevelopEnvironment()
	if !isDev {
		t.Errorf("Expected IsDevelopEnvironment() to return true, but got false")
	}
	os.Unsetenv(constants.EnvDevelopEnvironment)

	// Test case 4: Environment variable is set to "true"
	os.Setenv(constants.EnvDevelopEnvironment, "true")
	isDev = IsDevelopEnvironment()
	if !isDev {
		t.Errorf("Expected IsDevelopEnvironment() to return true, but got false")
	}
	os.Unsetenv(constants.EnvDevelopEnvironment)

	// Test case 5: Environment variable is set to any other value
	//os.Setenv(constants.EnvDevelopEnvironment, "false")
	//isDev = IsDevelopEnvironment()
	//if isDev {
	//	t.Errorf("Expected IsDevelopEnvironment() to return false, but got true")
	//}
}
