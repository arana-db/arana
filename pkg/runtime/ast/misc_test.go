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

package ast

import (
	"strings"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestWriteID(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, it := range []tt{
		{"simple", "`simple`"},
		{"你好,世界", "`你好,世界`"},
		{"Hello`World", "`Hello``World`"},
		{"it's ok", "`it's ok`"},
		{`x\y\z`, "`x\\y\\z`"},
	} {
		t.Run(it.input, func(t *testing.T) {
			var sb strings.Builder
			WriteID(&sb, it.input)
			assert.Equal(t, it.expect, sb.String())
		})
	}
}
