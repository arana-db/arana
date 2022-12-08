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

package rule

import (
	"fmt"
	"strings"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestShards(t *testing.T) {
	s := NewShards()
	s.Add(0, 0, 1, 2, 3)
	s.Add(1, 4, 5, 6, 7)
	s.Add(2, 8, 9, 10, 11)

	assert.Equal(t, 12, s.Len())
	t.Log("shards:", s)

	var sb strings.Builder
	s.Each(func(db, tb uint32) bool {
		sb.WriteString(fmt.Sprintf("%d.%d;", db, tb))
		return true
	})

	assert.Equal(t, "0.0;0.1;0.2;0.3;1.4;1.5;1.6;1.7;2.8;2.9;2.10;2.11;", sb.String())

	db, tb, ok := s.Min()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), db)
	assert.Equal(t, uint32(0), tb)

	db, tb, ok = s.Max()
	assert.True(t, ok)
	assert.Equal(t, uint32(2), db)
	assert.Equal(t, uint32(11), tb)
}
