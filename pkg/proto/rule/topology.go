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

// Topology represents the topology of databases and tables.
type Topology struct {
	dbRender, tbRender func(int) string
	idx                map[int][]int
}

// Len returns the length of database and table.
func (to *Topology) Len() (dbLen int, tblLen int) {
	dbLen = len(to.idx)
	for _, v := range to.idx {
		tblLen += len(v)
	}
	return
}

// SetTopology sets the topology.
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

// SetRender sets the database/table name render.
func (to *Topology) SetRender(dbRender, tbRender func(int) string) {
	to.dbRender, to.tbRender = dbRender, tbRender
}

// Render renders the name of database and table from indexes.
func (to *Topology) Render(dbIdx, tblIdx int) (string, string, bool) {
	if to.tbRender == nil || to.dbRender == nil {
		return "", "", false
	}
	return to.dbRender(dbIdx), to.tbRender(tblIdx), true
}

// Each enumerates items in current Topology.
func (to *Topology) Each(onEach func(dbIdx, tbIdx int) (ok bool)) bool {
	for d, v := range to.idx {
		for _, t := range v {
			if !onEach(d, t) {
				return false
			}
		}
	}
	return true
}
