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
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
	rrule "github.com/arana-db/arana/pkg/runtime/builtin"
	"github.com/arana-db/arana/pkg/runtime/calc/logic"
	"github.com/arana-db/arana/pkg/runtime/cmp"
)

func getMultipleKeysVTab() *rule.VTable {
	var vtab rule.VTable
	var topology rule.Topology
	topology.SetTopology(0, 0, 1, 2, 3)
	topology.SetTopology(1, 4, 5, 6, 7)
	topology.SetTopology(2, 8, 9, 10, 11)
	topology.SetTopology(3, 12, 13, 14, 15)
	vtab.SetTopology(&topology)

	var vs rule.VShard
	vs.DB = &rule.ShardMetadata{
		ShardColumns: []*rule.ShardColumn{
			{
				Name:  "uid",
				Steps: 16,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
			{
				Name:  "sid",
				Steps: 16,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
		},

		Computer: rrule.MustNewJavascriptShardComputer("parseInt((($0*31+$1) % 16)/4)", "uid", "sid"),
	}
	vs.Table = &rule.ShardMetadata{
		ShardColumns: []*rule.ShardColumn{
			{
				Name:  "uid",
				Steps: 16,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
			{
				Name:  "sid",
				Steps: 16,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
		},
		Computer: rrule.MustNewJavascriptShardComputer("parseInt(($0*31+$1) % 16)", "uid", "sid"),
	}
	vtab.AddVShards(&vs)

	return &vtab
}

func getSingleKeyVTab() *rule.VTable {
	var vtab rule.VTable
	var topology rule.Topology
	topology.SetTopology(0, 0, 1, 2, 3)
	topology.SetTopology(1, 4, 5, 6, 7)
	topology.SetTopology(2, 8, 9, 10, 11)
	topology.SetTopology(3, 12, 13, 14, 15)
	vtab.SetTopology(&topology)

	var vs rule.VShard
	vs.DB = &rule.ShardMetadata{
		ShardColumns: []*rule.ShardColumn{
			{
				Name:  "uid",
				Steps: 16,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
		},

		Computer: rrule.MustNewJavascriptShardComputer("parseInt(($0 % 16)/4)", "uid"),
	}
	vs.Table = &rule.ShardMetadata{
		ShardColumns: []*rule.ShardColumn{
			{
				Name:  "uid",
				Steps: 16,
				Stepper: rule.Stepper{
					N: 1,
					U: rule.Unum,
				},
			},
		},
		Computer: rrule.MustNewJavascriptShardComputer("parseInt($0 % 16)", "uid"),
	}
	vtab.AddVShards(&vs)

	return &vtab
}

func TestMultipleCalculus(t *testing.T) {
	type tt struct {
		scene string
		input logic.Logic[*Calculus]
		want  string
	}

	for _, next := range []tt{
		{
			"uid = 8 and sid = 1",
			logic.AND(
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 8)),
				Wrap(cmp.NewInt64("sid", cmp.Ceq, 1)),
			),

			func() string {
				const (
					uid uint32 = 8
					sid uint32 = 1
				)
				sh := rule.NewShards()
				h := uid*31 + sid
				sh.Add((h%16)/4, h%16)
				return sh.String()
			}(),
		},
		{
			"uid >= 8 and uid <= 10 and sid >= 1 and sid <= 3",
			logic.AND(
				logic.AND(
					Wrap(cmp.NewInt64("uid", cmp.Cgte, 8)),
					Wrap(cmp.NewInt64("uid", cmp.Clte, 10)),
				),
				logic.AND(
					Wrap(cmp.NewInt64("sid", cmp.Cgte, 1)),
					Wrap(cmp.NewInt64("sid", cmp.Clte, 3)),
				),
			),
			func() string {
				shards := rule.NewShards()
				for _, next := range [][2]int{
					{8, 1},
					{8, 2},
					{8, 3},
					{9, 1},
					{9, 2},
					{9, 3},
					{10, 1},
					{10, 2},
					{10, 3},
				} {
					h := uint32(next[0]*31 + next[1])
					shards.Add((h%16)/4, h%16)
				}
				return shards.String()
			}(),
		},
	} {
		t.Run(next.scene, func(t *testing.T) {
			shards, err := Eval(getMultipleKeysVTab(), next.input)
			assert.NoError(t, err)
			t.Logf("scene=%s, logic=%s, shards=%s\n", next.scene, next.input, shards.String())
			assert.Equal(t, next.want, shards.String())
		})
	}
}

func TestSingleCalculus(t *testing.T) {
	type tt struct {
		scene string
		input logic.Logic[*Calculus]
		want  string
	}

	for _, next := range []tt{
		{
			"uid = 8 and dummy = 1",
			logic.AND(
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 8)),
				Wrap(cmp.NewInt64("dummy", cmp.Ceq, 1)),
			),
			"[2:8]",
		},
		{
			"uid > 8 and uid <= 10",
			logic.AND(
				Wrap(cmp.NewInt64("uid", cmp.Cgt, 8)),
				Wrap(cmp.NewInt64("uid", cmp.Clte, 12)),
			),
			"[2:9,10,11;3:12]",
		},
		{
			"uid = 1 or uid = 2 or uid = 3",
			logic.OR(
				logic.OR(
					Wrap(cmp.NewInt64("uid", cmp.Ceq, 1)),
					Wrap(cmp.NewInt64("uid", cmp.Ceq, 2)),
				),
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 3)),
			),
			"[0:1,2,3]",
		},
		{
			"a = 1",
			Wrap(cmp.NewInt64("a", cmp.Ceq, 1)),
			"*",
		},
		{
			"not (uid = 1 or uid = 2)",
			logic.NOT(logic.OR(
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 1)),
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 2)),
			)),
			"*",
		},
		{
			"not uid = 1",
			logic.NOT(Wrap(cmp.NewInt64("uid", cmp.Ceq, 1))),
			"*",
		},
		{
			"uid == 1 and uid <> 1",
			logic.AND(
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 1)),
				Wrap(cmp.NewInt64("uid", cmp.Cne, 1)),
			),
			"[]",
		},
		{
			"uid == 1 and not (uid == 1)",
			logic.AND(
				Wrap(cmp.NewInt64("uid", cmp.Ceq, 1)),
				logic.NOT(Wrap(cmp.NewInt64("uid", cmp.Ceq, 1))),
			),
			"[]",
		},
	} {
		t.Run(next.scene, func(t *testing.T) {
			shards, err := Eval(getSingleKeyVTab(), next.input)
			if errors.Is(err, ErrNoShardMatched) {
				err = nil
			}
			assert.NoError(t, err)
			t.Logf("scene=%s, logic=%s, shards=%s\n", next.scene, next.input, shards.String())
			assert.Equal(t, next.want, shards.String())
		})
	}
}
