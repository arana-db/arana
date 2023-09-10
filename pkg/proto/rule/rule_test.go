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
	const (
		shardDBSize       = 4
		logicalVtableSize = 16
		stepperSize       = 1
	)

	topologyStatus := make(map[int][]int)

	// table topology: 4 databases, 16 tables
	for i := 0; i < logicalVtableSize; i++ {
		topologyStatus[i%shardDBSize] = append(topologyStatus[i%shardDBSize], i%logicalVtableSize)
	}

	t.Log("topology: ", topologyStatus)

	buildRule := func(c1, c2 ShardComputer) *Rule {
		var (
			ru      Rule
			vtab    VTable
			stepper = Stepper{
				N: stepperSize,
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
		for i := 0; i < logicalVtableSize; i++ {
			topo.SetTopology(i%shardDBSize, topologyStatus[i%shardDBSize]...)
		}

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
			return int(x) % logicalVtableSize / shardDBSize, nil
		}).
		MinTimes(1)
	c1.EXPECT().Variables().Return([]string{"uid"}).MinTimes(1)

	c2 := testdata.NewMockShardComputer(ctrl)
	c2.EXPECT().
		Compute(gomock.Any()).
		DoAndReturn(func(value proto.Value) (int, error) {
			x, _ := value.Int64()
			return int(x % logicalVtableSize), nil
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
	assert.Equal(t, uint32(42/logicalVtableSize), dbIdx)
	assert.Equal(t, uint32(42%logicalVtableSize), tblIdx)

	db, tbl, ok := vtab.Topology().Render(int(dbIdx), int(tblIdx))
	assert.True(t, ok)
	assert.Equal(t, "school_0002", db)
	assert.Equal(t, "student_0010", tbl)

	t.Logf("shard result: %s.%s\n", db, tbl)

	_, _, err = vtab.Shard(map[string]proto.Value{
		"name": proto.NewValueInt64(42),
	})
	assert.Error(t, err)

	// test table name invert functions
	phy2VtableMap := ru.GetInvertedPhyTableMap()
	expectedPhy2VtableMap := make(map[string]string, logicalVtableSize)
	for i := 0; i < logicalVtableSize; i++ {
		expectedPhy2VtableMap[fmt.Sprintf("student_%04d", i)] = "student"
	}
	assert.Equal(t, expectedPhy2VtableMap, phy2VtableMap)

	vtable2PhyMap := ru.GetInvertedVTableMap()
	expectedVtable2PhyMap := make(map[string][]string)
	for i := 0; i < logicalVtableSize; i++ {
		expectedVtable2PhyMap["student"] = append(expectedVtable2PhyMap["student"], fmt.Sprintf("student_%04d", i))
	}
	for tblName := range vtable2PhyMap {
		sort.Strings(vtable2PhyMap[tblName])
	}
	assert.Equal(t, expectedPhy2VtableMap, phy2VtableMap)

	ru.RemoveVTable("student")
	assert.False(t, ru.Has("student"))
	assert.False(t, (*Rule)(nil).Has("student"))
}
