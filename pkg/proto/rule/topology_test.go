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

func TestSetTopologyForIdxNil(t *testing.T) {
	topology := &Topology{
		idx: nil,
	}
	topology.SetTopology(2, 2, 3, 4)
	for each := range topology.idx {
		assert.Equal(t, 2, each)
		assert.Equal(t, 3, len(topology.idx[each]))
	}
	dbLen, tblLen := topology.Len()
	assert.Equal(t, 1, dbLen)
	assert.Equal(t, 3, tblLen)
}

func TestSetTopologyForIdxNotNil(t *testing.T) {
	topology := &Topology{
		idx: map[int][]int{0: []int{1, 2, 3}},
	}
	topology.SetTopology(1, 4, 5, 6)
	dbLen, tblLen := topology.Len()
	assert.Equal(t, 2, dbLen)
	assert.Equal(t, 6, tblLen)
}

func TestSetTopologyForTablesLessThanOne(t *testing.T) {
	topology := &Topology{
		idx: map[int][]int{0: []int{1, 2, 3}, 1: []int{4, 5, 6}},
	}
	topology.SetTopology(1)
	for each := range topology.idx {
		assert.Equal(t, 0, each)
		assert.Equal(t, 3, len(topology.idx[each]))
	}
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

func createTopology() *Topology {
	result := &Topology{
		dbRender: func(i int) string {
			return fmt.Sprintf("%s:%d", "dbRender", i)
		},
		tbRender: func(i int) string {
			return fmt.Sprintf("%s:%d", "tbRender", i)
		},
		idx: map[int][]int{0: []int{1, 2, 3}, 1: []int{4, 5, 6}},
	}
	return result
}
