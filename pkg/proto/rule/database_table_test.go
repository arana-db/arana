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
	"sort"
	"strings"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func parseDatabaseTablesFromString(s string) DatabaseTables {
	ret := make(DatabaseTables)
	sp := strings.Split(s, ";")
	for _, it := range sp {
		if strings.TrimSpace(it) == "" {
			continue
		}
		sp2 := strings.Split(it, ":")
		db := strings.TrimSpace(sp2[0])
		for _, tb := range strings.Split(sp2[1], ",") {
			ret[db] = append(ret[db], strings.TrimSpace(tb))
		}
	}

	for _, v := range ret {
		sort.Strings(v)
	}

	return ret
}

func TestDatabaseTables_And(t *testing.T) {
	for _, next := range [][3]string{
		{"foo:bar", "foo:bar,quz;bar:a", "foo:bar,bbb"},
		{"", "foo:bar", "bar:foo"},
		{"foo:bar", "foo:*", "foo:bar"},
		{"foo:bar", "*:bar", "foo:bar"},
		{"foo:bar", "*:bar", "foo:*"},
	} {
		var (
			should = parseDatabaseTablesFromString(next[0])
			a      = parseDatabaseTablesFromString(next[1])
			b      = parseDatabaseTablesFromString(next[2])
		)
		now := time.Now()
		res := a.And(b)
		cost := time.Since(now)
		t.Logf("%s AND %s = %s | cost=%s\n", a, b, res, cost)

		assert.Equal(t, should, res)
	}
}

func TestDatabaseTables_Or(t *testing.T) {
	for i, next := range [][3]string{
		{"foo:bar,quz,bbb;bar:a", "foo:bar,quz;bar:a", "foo:bar,bbb"},
		{"foo:bar;bar:foo", "foo:bar", "bar:foo"},
		{"foo:*", "foo:*", "foo:bar"},
		{"*:bar", "*:bar", "foo:bar"},
		{"*:bar;foo:*", "*:bar", "foo:*"},
	} {
		t.Run(fmt.Sprintf("C%d", i), func(t *testing.T) {
			var (
				should = parseDatabaseTablesFromString(next[0])
				a      = parseDatabaseTablesFromString(next[1])
				b      = parseDatabaseTablesFromString(next[2])
			)
			now := time.Now()
			res := a.Or(b)
			cost := time.Since(now)
			t.Logf("%s OR %s = %s | cost=%s\n", a, b, res, cost)

			assert.Equal(t, should, res)
		})
	}
}

func TestDatabaseTables_IsConfused(t *testing.T) {
	tests := []struct {
		name string
		dt   string
		want bool
	}{
		{"True1", "db0:tb0,tb1;db1:tb0,tb1", true},
		{"True2", "db0:tb0,tb1;db1:tb0,tb2", true},
		{"False", "db0:tb0,tb1;db1:tb2,tb3", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := parseDatabaseTablesFromString(tt.dt)
			got := dt.IsConfused()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDatabaseTables_Largest(t *testing.T) {
	type tt struct {
		input               string
		expectDb, expectTbl string
	}

	for _, it := range []tt{
		{"db0:tb0,tb1;db1:tb2,tb3;db2:tb4,tb5", "db2", "tb5"},
		{"db2:tb0,tb1;db1:tb2,tb3;db0:tb4,tb5", "db0", "tb5"},
	} {
		t.Run(it.input, func(t *testing.T) {
			dt := parseDatabaseTablesFromString(it.input)
			db, tbl := dt.Largest()
			assert.Equal(t, it.expectTbl, tbl)
			assert.Equal(t, it.expectDb, db)
		})
	}
}

func TestDatabaseTables_Smallest(t *testing.T) {
	type tt struct {
		input               string
		expectDb, expectTbl string
	}

	for _, it := range []tt{
		{"db0:tb0,tb1;db1:tb2,tb3;db2:tb4,tb5", "db0", "tb0"},
		{"db2:tb0,tb1;db1:tb2,tb3;db0:tb4,tb5", "db2", "tb0"},
	} {
		t.Run(it.input, func(t *testing.T) {
			dt := parseDatabaseTablesFromString(it.input)
			db, tbl := dt.Smallest()
			assert.Equal(t, it.expectTbl, tbl)
			assert.Equal(t, it.expectDb, db)
		})
	}
}

func TestDatabaseTables_Replace(t *testing.T) {
	type tt struct {
		input     string
		expectTbl string
	}

	for _, it := range []tt{
		{"db0:tb0,tb1;db1:tb2,tb3;db2:tb4,tb5", "tb0"},
		{"db2:tb0,tb1;db1:tb2,tb3;db0:tb4,tb5", "tb0"},
	} {
		t.Run(it.input, func(t *testing.T) {
			dt := parseDatabaseTablesFromString(it.input)
			dt.ReplaceDb("shadow")
			assert.Equal(t, 6, len(dt["shadow"]))
			assert.Equal(t, 1, len(dt))
		})
	}
}
