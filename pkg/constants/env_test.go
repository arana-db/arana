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

package constants

import (
	"strings"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestGetConfigSearchPathList(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{"Test", []string{".", "./conf", "/Users/dongzonglei/.arana", "/etc/arana"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetConfigSearchPathList()
			assert.Equal(t, 4, len(result))
			assert.Equal(t, ".", result[0])
			assert.Equal(t, "./conf", result[1])
			assert.True(t, strings.HasSuffix(result[2], ".arana"))
			assert.Equal(t, "/etc/arana", result[3])
		})
	}
}
