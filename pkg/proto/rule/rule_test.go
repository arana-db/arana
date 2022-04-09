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
	"github.com/arana-db/arana/testdata"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
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

		vtab.SetShardMetadata("uid", &ShardMetadata{
			Stepper:  stepper,
			Computer: c1,
		}, &ShardMetadata{
			Stepper:  stepper,
			Computer: c2,
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
		DoAndReturn(func(value interface{}) (int, error) {
			return value.(int) % 4, nil
		}).
		MinTimes(1)

	c2 := testdata.NewMockShardComputer(ctrl)
	c2.EXPECT().
		Compute(gomock.Any()).
		DoAndReturn(func(value interface{}) (int, error) {
			return value.(int) % 16, nil
		}).
		MinTimes(1)

	ru := buildRule(c1, c2)

	assert.True(t, ru.Has("student"))
	assert.False(t, ru.Has("fake_table"))

	assert.True(t, ru.HasColumn("student", "uid"))
	assert.False(t, ru.HasColumn("student", "fake_field"))
	assert.False(t, ru.HasColumn("fake_table", "uid"))
	assert.False(t, ru.HasColumn("fake_table", "fake_field"))

	vtab := ru.MustVTable("student")

	var ok bool
	_, _, ok = vtab.GetShardMetadata("name")
	assert.False(t, ok)
	_, _, ok = vtab.GetShardMetadata("fake_field")
	assert.False(t, ok)
	_, _, ok = vtab.GetShardMetadata("uid")
	assert.True(t, ok)

	assert.Len(t, vtab.GetShardKeys(), 1, "length shard keys should be 1")

	dbIdx, tblIdx, err := vtab.Shard("uid", 42)
	assert.NoError(t, err)
	assert.Equal(t, 2, dbIdx)
	assert.Equal(t, 10, tblIdx)

	db, tbl, ok := vtab.Topology().Render(dbIdx, tblIdx)
	assert.True(t, ok)

	t.Logf("shard result: %s.%s\n", db, tbl)

	_, _, noDataErr := vtab.Shard("name", 42)
	assert.Error(t, noDataErr)

	ru.RemoveVTable("student")
	assert.False(t, ru.Has("student"))
	assert.False(t, (*Rule)(nil).Has("student"))
}

func TestDirectShardComputer_Compute(t *testing.T) {
	dc := DirectShardComputer(func(i interface{}) (int, error) {
		n, _ := strconv.Atoi(fmt.Sprintf("%v", i))
		return n % 32, nil
	})
	res, err := dc.Compute(33)
	assert.NoError(t, err)
	assert.Equal(t, 1, res, "should compute correctly")
}
