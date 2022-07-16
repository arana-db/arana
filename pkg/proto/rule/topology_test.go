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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestLen(t *testing.T) {
	topology := createTopology()
	dbLen, tblLen := topology.Len()
	assert.Equal(t, 2, dbLen)
	assert.Equal(t, 6, tblLen)
}

func TestSetTopology(t *testing.T) {
	var topology Topology

	topology.SetTopology(2, 2, 3, 4)
	dbLen, tblLen := topology.Len()
	assert.Equal(t, 1, dbLen)
	assert.Equal(t, 3, tblLen)
}

func TestSetTopologyNoConflict(t *testing.T) {
	var topology Topology
	topology.SetTopology(0, 1, 2, 3)
	topology.SetTopology(1, 4, 5, 6)
	dbLen, tblLen := topology.Len()
	assert.Equal(t, 2, dbLen)
	assert.Equal(t, 6, tblLen)
}

func TestSetTopologyForTablesLessThanOne(t *testing.T) {
	var topology Topology

	topology.SetTopology(0, 1, 2, 3)
	topology.SetTopology(1, 4, 5, 6)
	topology.SetTopology(1)

	dbLen, tblLen := topology.Len()
	assert.Equal(t, 1, dbLen)
	assert.Equal(t, 3, tblLen)
}

func TestSetRender(t *testing.T) {
	topology := &Topology{}
	topology.SetRender(func(i int) string {
		return fmt.Sprintf("%s:%d", "dbRender", i)
	}, func(i int) string {
		return fmt.Sprintf("%s:%d", "tbRender", i)
	})
	assert.Equal(t, "dbRender:1024", topology.dbRender(1024))
	assert.Equal(t, "tbRender:2048", topology.tbRender(2048))
}

func TestRenderForRenderNil(t *testing.T) {
	topology := &Topology{}
	dbRender, tbRender, ok := topology.Render(8, 32)
	assert.Empty(t, dbRender)
	assert.Empty(t, tbRender)
	assert.False(t, ok)
}

func TestRenderForRenderNotNil(t *testing.T) {
	topology := createTopology()
	dbRender, tbRender, ok := topology.Render(8, 32)
	assert.Equal(t, "dbRender:8", dbRender)
	assert.Equal(t, "tbRender:32", tbRender)
	assert.True(t, ok)
}

func TestTopology_Each(t *testing.T) {
	topology := createTopology()
	topology.Each(func(dbIdx, tbIdx int) bool {
		t.Logf("on each: %d,%d\n", dbIdx, tbIdx)
		return true
	})

	assert.False(t, topology.Each(func(dbIdx, tbIdx int) bool {
		return false
	}))
}

func TestTopology_Enumerate(t *testing.T) {
	topology := createTopology()
	shards := topology.Enumerate()
	assert.Greater(t, shards.Len(), 0)
}

func TestTopology_EnumerateDatabases(t *testing.T) {
	topology := createTopology()
	dbs := topology.EnumerateDatabases()
	assert.Greater(t, len(dbs), 0)
}

func TestTopology_Largest(t *testing.T) {
	topology := createTopology()
	db, tb, ok := topology.Largest()
	assert.True(t, ok)
	t.Logf("largest: %s.%s\n", db, tb)
}

func TestTopology_Smallest(t *testing.T) {
	topology := createTopology()
	db, tb, ok := topology.Smallest()
	assert.True(t, ok)
	t.Logf("smallest: %s.%s\n", db, tb)
}

func createTopology() *Topology {
	result := &Topology{
		dbRender: func(i int) string {
			return fmt.Sprintf("%s:%d", "dbRender", i)
		},
		tbRender: func(i int) string {
			return fmt.Sprintf("%s:%d", "tbRender", i)
		},
	}

	result.SetTopology(0, 1, 2, 3)
	result.SetTopology(1, 4, 5, 6)

	return result
}
