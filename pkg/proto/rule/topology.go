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
	"sort"
)

// Topology 定义了分库分表物理拓扑
type Topology struct {
	dbRender, tbRender func(int) string
	idx                map[int][]int
}

func (to *Topology) Len() (dbLen int, tblLen int) {
	dbLen = len(to.idx)
	for _, v := range to.idx {
		tblLen += len(v)
	}
	return
}

func (to *Topology) SetTopology(db int, tables ...int) {
	if to.idx == nil {
		to.idx = make(map[int][]int)
	}

	if len(tables) < 1 {
		delete(to.idx, db)
		return
	}

	clone := make([]int, len(tables))
	copy(clone, tables)
	sort.Ints(clone)
	to.idx[db] = clone
}

func (to *Topology) SetRender(dbRender, tbRender func(int) string) {
	to.dbRender, to.tbRender = dbRender, tbRender
}

func (to *Topology) Render(dbIdx, tblIdx int) (string, string, bool) {
	if to.tbRender == nil || to.dbRender == nil {
		return "", "", false
	}
	return to.dbRender(dbIdx), to.tbRender(tblIdx), true
}
