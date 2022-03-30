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

package function

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestScripts_Time(t *testing.T) {
	for _, it := range []string{
		"UNIX_TIMESTAMP", "NOW", "CURTIME", "CURDATE", "CURRENT_DATE", "SYSDATE",
	} {
		v, err := testFunc(it)
		assert.NoError(t, err)
		t.Logf("%s: %v\n", it, v)
	}

	now := time.Now()

	v, err := testFunc("FROM_UNIXTIME", now.Unix()+24*3600)
	assert.NoError(t, err)
	t.Log("FROM_UNIXTIME:", v)

	v, err = testFunc("MONTHNAME", now)
	assert.NoError(t, err)
	t.Log("MONTHNAME:", v)

	v, err = testFunc("ADDDATE", now, 1)
	assert.NoError(t, err)
	t.Log("ADDDATE:", v)

	v, err = testFunc("SUBDATE", now, 1)
	assert.NoError(t, err)
	t.Log("SUBDATE:", v)

	v, err = testFunc("DATE_FORMAT", now, "%Y-%m-%d")
	assert.NoError(t, err)
	t.Log("DATE_FORMAT:", v)

}

func TestTime_Month(t *testing.T) {
	var (
		now = time.Now()
		v   interface{}
		err error
	)

	v, err = testFunc("MONTH", now)
	assert.NoError(t, err)
	assert.Equal(t, int64(now.Month()), v)

	v, err = testFunc("MONTH", "2021-03-08")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), v)
}

func TestTime_Day(t *testing.T) {
	var (
		now = time.Now()
		v   interface{}
		err error
	)

	v, err = testFunc("DAY", now)
	assert.NoError(t, err)
	assert.Equal(t, int64(now.Day()), v)

	v, err = testFunc("DAY", "2021-03-14")
	assert.NoError(t, err)
	assert.Equal(t, int64(14), v)
}

func TestScripts_Math(t *testing.T) {
	for _, it := range []*table{
		newTable(int64(1), "ABS", -1),
		newTable(int64(4), "CEIL", 3.14),
		newTable(int64(3), "FLOOR", 3.14),
	} {
		it.Do(t)
	}

	for _, next := range []string{"RAND", "PI"} {
		v, err := testFunc(next)
		assert.NoError(t, err)
		t.Logf("%s: %v\n", next, v)
	}
}

func TestScripts_Crypto(t *testing.T) {
	for _, it := range []*table{
		newTable("356a192b7913b04c54574d18c28d46e6395428ab", "SHA", "1"),
		newTable("356a192b7913b04c54574d18c28d46e6395428ab", "SHA", 1),
		newTable("8843d7f92416211de9ebb963ff4ce28125932878", "SHA", "foobar"),
		newTable("3858f62230ac3c915f300c664312c63f", "MD5", "foobar"),
		newTable("9ef9f83efc764b649d5b80a975da48c9", "MD5", time.Unix(0, 0)),
	} {
		it.Do(t)
	}
}

func TestScripts_String(t *testing.T) {
	for _, it := range []*table{
		newTable("abcd", "CONCAT", "ab", "c", "d"),
		newTable("ab", "CONCAT_WS", ",", "ab"),
		newTable("ab,c,de", "CONCAT_WS", ",", "ab", "c", "de"),
		newTable(int64(5), "CHAR_LENGTH", "你好123"),
		newTable(int64(9), "LENGTH", "你好123"),
		newTable("ABC", "UPPER", "abc"),
		newTable("abc", "LOWER", "Abc"),
		newTable("a", "LEFT", "abc", 1),
		newTable("bc", "RIGHT", "abc", 2),
		newTable("aaaa", "REPEAT", "a", 4),
		newTable("    ", "SPACE", 4),
		newTable("xbcxbc", "REPLACE", "abcabc", "a", "x"),
		newTable(int64(1), "STRCMP", "foo", "1234"),
		newTable(int64(-1), "STRCMP", "bar", "foo"),
		newTable("olleh", "REVERSE", "hello"),
		newTable("abc", "SUBSTRING", "abcdef", 1, 3),
		newTable("", "SUBSTRING", "abcdef", 0, 3),
		newTable("", "SUBSTRING", "abcdef", 1, 0),
		newTable("cde", "SUBSTRING", "abcdef", -4, 3),
		newTable("0001", "LPAD", "1", 4, "0"),
		newTable("abc   ", "LTRIM", "   abc   "),
		newTable("   abc", "RTRIM", "   abc   "),
	} {
		it.Do(t)
	}
}

func TestScripts_If(t *testing.T) {
	for _, it := range []*table{
		newTable("a", "IF", true, "a", "b"),
		newTable("b", "IF", false, "a", "b"),
		newTable("b", "IFNULL", nil, "b"),
		newTable("a", "IFNULL", "a", "b"),
	} {
		it.Do(t)
	}
}

func testFunc(name string, args ...interface{}) (interface{}, error) {
	var sb strings.Builder
	sb.WriteString("$")
	sb.WriteString(name)
	sb.WriteByte('(')
	if len(args) > 0 {
		sb.WriteString("arguments[0]")
		for i := 1; i < len(args); i++ {
			sb.WriteByte(',')
			sb.WriteString("arguments[")
			sb.WriteString(strconv.FormatInt(int64(i), 10))
			sb.WriteByte(']')
		}
	}
	sb.WriteByte(')')

	vm := BorrowVM()
	defer ReturnVM(vm)
	return vm.Eval(sb.String(), args)
}

type table struct {
	F string
	E interface{}
	A []interface{}
}

func (tab *table) Do(t *testing.T) {
	v, err := testFunc(tab.F, tab.A...)
	assert.NoError(t, err, "should eval correctly")
	assert.Equal(t, tab.E, v, "values should be match")
}

func newTable(expect interface{}, name string, args ...interface{}) *table {
	return &table{
		F: name,
		E: expect,
		A: args,
	}
}
