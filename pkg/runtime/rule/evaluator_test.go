// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package rule

import (
	"fmt"
	"strconv"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/proto/rule"
	"github.com/dubbogo/arana/pkg/runtime/cmp"
)

const (
	fakeDB    = "fake_db"
	fakeTable = "fake_table"
)

type simpleModComputer int

func (s simpleModComputer) Compute(value interface{}) (int, error) {
	n, err := strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(n) % int(s), nil
}

func makeTestRule(mod int) *rule.Rule {
	var (
		vt   rule.VTable
		topo rule.Topology
	)
	var s []int
	for i := 0; i < mod; i++ {
		s = append(s, i)
	}
	topo.SetTopology(0, s...) // 单库多表
	topo.SetRender(func(_ int) string {
		return fakeDB
	}, func(i int) string {
		return fmt.Sprintf("%s_%04d", fakeTable, i)
	})

	vt.SetTopology(&topo)

	sm := &rule.ShardMetadata{
		Stepper: rule.Stepper{
			N: 1,
			U: rule.Unum,
		},
		Computer: simpleModComputer(mod),
	}

	vt.SetShardMetadata("uid", nil, sm)

	var ru rule.Rule
	ru.SetVTable(fakeTable, &vt)
	return &ru
}

func TestEvaluator_Eval(t *testing.T) {
	var (
		ru4 = makeTestRule(4)
		ru8 = makeTestRule(8)
	)

	t.Run("Basic", func(t *testing.T) {
		a := NewKeyed("uid", cmp.Cgte, 10).ToLogical()
		b := NewKeyed("uid", cmp.Clte, 13).ToLogical()
		c := a.And(b)

		v, err := Eval(c, fakeTable, ru8)
		assert.NoError(t, err)
		res, err := v.Eval(fakeTable, ru8)
		assert.NoError(t, err)
		// 2,3,4,5
		t.Log("result:", res)
	})

	t.Run("LogicTuning", func(t *testing.T) {
		// "select * from `tb_user` where (id > 1 or uid = 10003 ) and (id > 1 or uid = 10004 or uid = 10005)";
		k1 := NewKeyed("id", cmp.Cgt, 1).ToLogical()
		k2 := NewKeyed("uid", cmp.Ceq, 10003).ToLogical()
		k3 := NewKeyed("id", cmp.Cgt, 1).ToLogical()
		k4 := NewKeyed("uid", cmp.Ceq, 10004).ToLogical()
		k5 := NewKeyed("uid", cmp.Ceq, 10005).ToLogical()

		l := k1.Or(k2).And(k3.Or(k4).Or(k5))

		// 逻辑运算优化, 最终优化为 id > 1
		// (id > 1 or uid = 10003 ) and (id > 1 or uid = 10004 or uid = 10005)
		// ( id > 1 OR ( uid = 10003 AND uid = 10005 ) OR ( uid = 10003 AND uid = 10004 ) )
		// id > 1
		v, err := Eval(l, fakeTable, ru4)
		assert.NoError(t, err)
		assert.Equal(t, "id > 1", v.(fmt.Stringer).String())
	})

	t.Run("AlwayFalse", func(t *testing.T) {
		l1 := NewKeyed("uid", cmp.Cgte, 4).ToLogical()
		l2 := NewKeyed("uid", cmp.Cgte, 7).ToLogical()
		l3 := NewKeyed("uid", cmp.Clt, 8).ToLogical()

		l := l1.And(l2).And(l3) // 永假: uid >= 4 AND uid >= 7 AND uid < 8

		v, err := Eval(l, "fake_table", ru4)
		assert.NoError(t, err)
		t.Logf("%s => %s", l.ToString("AND", "OR"), v)
	})

}
