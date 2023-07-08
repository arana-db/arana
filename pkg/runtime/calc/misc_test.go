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

package calc

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/cmp"
)

func TestComputeRange(t *testing.T) {
	m := &rule.ShardMetadata{
		ShardColumns: []*rule.ShardColumn{
			{
				Name:  "x",
				Steps: 8,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
		},
	}

	type tt struct {
		scene      string
		begin, end *cmp.Comparative
		expect     []interface{}
	}

	for _, next := range []tt{
		{
			"x>=1 && x<=4",
			cmp.NewInt64("x", cmp.Cgte, 1),
			cmp.NewInt64("x", cmp.Clte, 4),
			[]interface{}{int64(1), int64(2), int64(3), int64(4)},
		},
		{
			"x>=1 && x<4",
			cmp.NewInt64("x", cmp.Cgte, 1),
			cmp.NewInt64("x", cmp.Clt, 4),
			[]interface{}{int64(1), int64(2), int64(3)},
		},
	} {
		t.Run(next.scene, func(t *testing.T) {
			actual := computeRange(m, next.begin, next.end)
			assert.Equal(t, next.expect, actual)
		})
	}
}
