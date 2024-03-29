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

package file

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestIsYaml(t *testing.T) {
	// Test with a valid YAML file
	path := "example.yaml"
	result := IsYaml(path)
	assert.True(t, result)

	// Test with a valid YML file
	path = "example.yml"
	result = IsYaml(path)
	assert.True(t, result)

	// Test with an invalid file extension
	path = "example.txt"
	result = IsYaml(path)
	assert.False(t, result)
}
