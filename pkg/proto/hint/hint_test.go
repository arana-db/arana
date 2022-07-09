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

package hint

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	type tt struct {
		input  string
		output string
		pass   bool
	}

	for _, next := range []tt{
		{"route( foo , bar , qux )", "ROUTE(foo,bar,qux)", true},
		{"master", "MASTER()", true},
		{"slave", "SLAVE()", true},
		{"not_exist_hint(1,2,3)", "", false},
		{"route(,,,)", "ROUTE()", true},
		{"fullscan()", "FULLSCAN()", true},
		{"route(foo=111,bar=222,qux=333,)", "ROUTE(foo=111,bar=222,qux=333)", true},
	} {
		t.Run(next.input, func(t *testing.T) {
			res, err := Parse(next.input)
			if next.pass {
				assert.NoError(t, err)
				assert.Equal(t, next.output, res.String())
			} else {
				assert.Error(t, err)
			}
		})
	}
}
