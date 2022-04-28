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
	"strconv"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestBadScript(t *testing.T) {
	_, err := NewJavascriptShardComputer(")))BAD(((")
	assert.Error(t, err)
}

func TestScriptRuntimeThrow(t *testing.T) {
	c, err := NewJavascriptShardComputer(`
if ($value < 0) {
  throw new Error('oops');
}
return $value;
`)

	assert.NoError(t, err)
	_, err = c.Compute(0)
	assert.NoError(t, err)
	_, err = c.Compute(-1)
	assert.Error(t, err)
}

func TestSimpleScript(t *testing.T) {
	// tables: 4*32
	var (
		db, _ = NewJavascriptShardComputer("($value % 128) / 32")
		tb, _ = NewJavascriptShardComputer("$value % 128")
	)

	type tt struct {
		input   int
		db, tbl int
	}

	for _, it := range []tt{
		{1, 0, 1},     // DB_0000.TBL_0001
		{16, 0, 16},   // DB_0000.TBL_0016
		{32, 1, 32},   // DB_0001.TBL_0032
		{100, 3, 100}, // DB_0003.TBL_0100
		{128, 0, 0},   // DB_0000.TBL_0001
		{129, 0, 1},   // DB_0000.TBL_0001
	} {
		t.Run(strconv.Itoa(it.input), func(t *testing.T) {
			v, err := db.Compute(it.input)
			assert.NoError(t, err)
			assert.Equal(t, it.db, v)
			v, err = tb.Compute(it.input)
			assert.NoError(t, err)
			assert.Equal(t, it.tbl, v)
		})
	}
}

func TestComplexScript(t *testing.T) {
	script := `
// throw error if value is not string:
if ( typeof $value !== 'string') {
  return 0;
}

// return zero if length is not enough
if ($value.length < 8) {
  return 0;
}

let n = parseInt($value.substring(2, 8));
if (isNaN(n)) {
  return 0;
}

return n%32;
`

	c, err := NewJavascriptShardComputer(script)
	assert.NoError(t, err)

	type tt struct {
		input  interface{}
		output int
	}

	for _, it := range []tt{
		{1234, 0},          // not string -> 0
		{"SN7777", 0},      // length<8 -> 0
		{"SN000042CN", 10}, // 42%32 -> 10
		{"SNxxxxxxJP", 0},  // NaN -> 0
	} {
		t.Run(fmt.Sprint(it.input), func(t *testing.T) {
			actual, err := c.Compute(it.input)
			assert.NoError(t, err)
			assert.Equal(t, it.output, actual)
		})
	}
}

func BenchmarkJavascriptShardComputer(b *testing.B) {
	computer, _ := NewJavascriptShardComputer("$value % 32")
	_, _ = computer.Compute(42)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = computer.Compute(42)
		}
	})
}
