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
	"reflect"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/testdata"
)

func TestRule(t *testing.T) {
	m := make(map[int][]int)

	for i := 0; i < 16; i++ {
		m[i%4] = append(m[i%4], i%16)
	}

	t.Log(m)

	buildRule := func(c1, c2 ShardComputer) *Rule {
		var (
			ru      Rule
			vtab    VTable
			stepper = Stepper{
				N: 1,
				U: Unum,
			}
		)

		vtab.AddVShards(&VShard{
			DB: &ShardMetadata{
				ShardColumns: []*ShardColumn{
					{
						Name:    "uid",
						Stepper: stepper,
					},
				},
				Computer: c1,
			},
			Table: &ShardMetadata{
				ShardColumns: []*ShardColumn{
					{
						Name:    "uid",
						Stepper: stepper,
					},
				},
				Computer: c2,
			},
		})

		// table topology: 4 databases, 16 tables
		var topo Topology
		topo.SetTopology(0, 0, 4, 8, 12)
		topo.SetTopology(1, 1, 5, 9, 13)
		topo.SetTopology(2, 2, 6, 10, 14)
		topo.SetTopology(3, 3, 7, 11, 15)
		topo.SetRender(func(i int) string {
			return fmt.Sprintf("school_%04d", i)
		}, func(i int) string {
			return fmt.Sprintf("student_%04d", i)
		})

		vtab.SetTopology(&topo)

		ru.SetVTable("student", &vtab)
		return &ru
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c1 := testdata.NewMockShardComputer(ctrl)
	c1.EXPECT().
		Compute(gomock.Any()).
		DoAndReturn(func(value proto.Value) (int, error) {
			x, _ := value.Int64()
			return int(x) % 16 / 4, nil
		}).
		MinTimes(1)
	c1.EXPECT().Variables().Return([]string{"uid"}).MinTimes(1)

	c2 := testdata.NewMockShardComputer(ctrl)
	c2.EXPECT().
		Compute(gomock.Any()).
		DoAndReturn(func(value proto.Value) (int, error) {
			x, _ := value.Int64()
			return int(x % 16), nil
		}).
		MinTimes(1)
	c2.EXPECT().Variables().Return([]string{"uid"}).MinTimes(1)

	ru := buildRule(c1, c2)

	assert.True(t, ru.Has("student"))
	assert.False(t, ru.Has("fake_table"))

	assert.True(t, ru.MustVTable("student").HasVShard("uid"))
	assert.False(t, ru.MustVTable("student").HasVShard("fake_field"))

	vtab := ru.MustVTable("student")

	assert.Equal(t, 1, vtab.GetVShards()[0].Len(), "length shard keys should be 1")

	dbIdx, tblIdx, err := vtab.Shard(map[string]proto.Value{
		"uid": proto.NewValueInt64(42),
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), dbIdx)
	assert.Equal(t, uint32(10), tblIdx)

	db, tbl, ok := vtab.Topology().Render(int(dbIdx), int(tblIdx))
	assert.True(t, ok)
	assert.Equal(t, "school_0002", db)
	assert.Equal(t, "student_0010", tbl)

	t.Logf("shard result: %s.%s\n", db, tbl)

	_, _, err = vtab.Shard(map[string]proto.Value{
		"name": proto.NewValueInt64(42),
	})
	assert.Error(t, err)

	ru.RemoveVTable("student")
	assert.False(t, ru.Has("student"))
	assert.False(t, (*Rule)(nil).Has("student"))
}

func TestGetShardColumn_Found(t *testing.T) {
	sm := ShardMetadata{
		ShardColumns: []*ShardColumn{
			{Name: "column1"},
			{Name: "column2"},
			{Name: "column3"},
		},
	}

	name := "column2"
	expected := &ShardColumn{Name: "column2"}
	result := sm.GetShardColumn(name)

	assert.True(t, reflect.DeepEqual(result, expected))
}

func TestGetShardColumn_NotFound(t *testing.T) {
	sm := ShardMetadata{
		ShardColumns: []*ShardColumn{
			{Name: "column1"},
			{Name: "column2"},
			{Name: "column3"},
		},
	}

	name := "column4"
	expected := (*ShardColumn)(nil)
	result := sm.GetShardColumn(name)

	assert.Nil(t, result)
	assert.True(t, reflect.DeepEqual(result, expected))
}
