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

package optimize

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/testdata"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// test rule: student, uid % 8
	fakeRule := makeFakeRule(ctrl, 8)

	type tt struct {
		sql    string
		args   []interface{}
		expect []int
	}

	for _, it := range []tt{
		{"select * from student where uid = ? or uid = ?", []interface{}{7, 12}, []int{4, 7}},
		{"select * from student where uid = if(PI()>3, 1, 0)", nil, []int{1}},
		{"select * from student where uid = if(PI()<3, 1, ?)", []interface{}{0}, []int{0}},
	} {
		t.Run(it.sql, func(t *testing.T) {
			stmt := ast.MustParse(it.sql).(*ast.SelectStatement)

			result, _, err := (*Sharder)(fakeRule).Shard(stmt.From[0].TableName(), stmt.Where, it.args...)
			assert.NoError(t, err, "shard failed")

			var sb strings.Builder
			sort.Ints(it.expect)
			sb.WriteByte('[')
			_, _ = fmt.Fprintf(&sb, `"fake_db.student_%04d"`, it.expect[0])
			for i := 1; i < len(it.expect); i++ {
				sb.WriteByte(',')
				sb.WriteByte(' ')
				_, _ = fmt.Fprintf(&sb, `"fake_db.student_%04d"`, it.expect[i])
			}
			sb.WriteByte(']')

			assert.Equal(t, sb.String(), result.String(), "bad shard result")

			t.Log("shard result:", result)
		})
	}
}

func makeFakeRule(c *gomock.Controller, mod int) *rule.Rule {
	var (
		ru   rule.Rule
		tab  rule.VTable
		topo rule.Topology
	)

	topo.SetRender(func(_ int) string {
		return "fake_db"
	}, func(i int) string {
		return fmt.Sprintf("student_%04d", i)
	})

	tables := make([]int, 0, mod)
	for i := 0; i < mod; i++ {
		tables = append(tables, i)
	}
	topo.SetTopology(0, tables...)

	tab.SetTopology(&topo)

	computer := testdata.NewMockShardComputer(c)

	computer.EXPECT().
		Compute(gomock.Any()).
		DoAndReturn(func(value interface{}) (int, error) {
			n, err := strconv.Atoi(fmt.Sprintf("%v", value))
			if err != nil {
				return 0, err
			}
			return n % mod, nil
		}).
		MinTimes(1)

	var sm rule.ShardMetadata
	sm.Steps = 8
	sm.Computer = computer

	tab.SetShardMetadata("uid", nil, &sm)
	ru.SetVTable("student", &tab)
	return &ru
}
