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

package optimize_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	_ "github.com/arana-db/arana/pkg/runtime/function"
	. "github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/testdata"
)

func TestShardNG(t *testing.T) {
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
		{"select * from student where uid = PI() div 3", nil, []int{1}},
		{"select * from student where uid = PI() div ?", []interface{}{3}, []int{1}},
		{"select * from student where 1+2", nil, nil},
	} {
		t.Run(it.sql, func(t *testing.T) {
			_, rawStmt := ast.MustParse(it.sql)
			stmt := rawStmt.(*ast.SelectStatement)

			args := make([]proto.Value, 0, len(it.args))
			for i := range it.args {
				arg, err := proto.NewValue(it.args[i])
				assert.NoError(t, err)
				args = append(args, arg)
			}

			shd := NewXSharder(context.TODO(), fakeRule, args)

			shards, err := stmt.Accept(shd)
			assert.NoError(t, err)
			t.Log("shards:", shards)
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
	tab.SetName("student")

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
