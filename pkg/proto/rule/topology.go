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
	"math"
	"sort"
	"sync"
)

// Topology represents the topology of databases and tables.
type Topology struct {
	mu                 sync.RWMutex
	dbRender, tbRender func(int) string
	idx                sync.Map // map[int][]int
}

// Len returns the length of database and table.
func (to *Topology) Len() (dbLen int, tblLen int) {
	to.idx.Range(func(_, value any) bool {
		dbLen++
		tblLen += len(value.([]int))
		return true
	})
	return
}

// SetTopology sets the topology.
func (to *Topology) SetTopology(db int, tables ...int) {
	if len(tables) < 1 {
		to.idx.Delete(db)
		return
	}

	clone := make([]int, len(tables))
	copy(clone, tables)
	sort.Ints(clone)
	to.idx.Store(db, clone)
}

// SetRender sets the database/table name render.
func (to *Topology) SetRender(dbRender, tbRender func(int) string) {
	to.mu.Lock()
	to.dbRender, to.tbRender = dbRender, tbRender
	to.mu.Unlock()
}

// Render renders the name of database and table from indexes.
func (to *Topology) Render(dbIdx, tblIdx int) (string, string, bool) {
	to.mu.RLock()
	defer to.mu.RUnlock()

	if to.tbRender == nil || to.dbRender == nil {
		return "", "", false
	}
	return to.dbRender(dbIdx), to.tbRender(tblIdx), true
}

func (to *Topology) EnumerateDatabases() []string {
	to.mu.RLock()
	render := to.dbRender
	to.mu.RUnlock()

	var keys []string

	to.idx.Range(func(key, _ any) bool {
		keys = append(keys, render(key.(int)))
		return true
	})

	sort.Strings(keys)

	return keys
}

func (to *Topology) Enumerate() DatabaseTables {
	to.mu.RLock()
	dbRender, tbRender := to.dbRender, to.tbRender
	to.mu.RUnlock()

	dt := make(DatabaseTables)
	to.Each(func(dbIdx, tbIdx int) bool {
		d := dbRender(dbIdx)
		t := tbRender(tbIdx)
		dt[d] = append(dt[d], t)
		return true
	})

	return dt
}

// Each enumerates items in current Topology.
func (to *Topology) Each(onEach func(dbIdx, tbIdx int) (ok bool)) bool {
	done := true
	to.idx.Range(func(key, value any) bool {
		var (
			d = key.(int)
			v = value.([]int)
		)
		for _, t := range v {
			if !onEach(d, t) {
				done = false
				return false
			}
		}
		return true
	})

	return done
}

func (to *Topology) Smallest() (db, tb string, ok bool) {
	to.mu.RLock()
	dbRender, tbRender := to.dbRender, to.tbRender
	to.mu.RUnlock()

	smallest := [2]int{math.MaxInt64, math.MaxInt64}
	to.idx.Range(func(key, value any) bool {
		if d := key.(int); d < smallest[0] {
			smallest[0] = d
			if t := value.([]int); len(t) > 0 {
				smallest[1] = t[0]
			}
		}
		return true
	})

	if smallest[0] != math.MaxInt64 || smallest[1] != math.MaxInt64 {
		db, tb, ok = dbRender(smallest[0]), tbRender(smallest[1]), true
	}

	return
}

func (to *Topology) Largest() (db, tb string, ok bool) {
	to.mu.RLock()
	dbRender, tbRender := to.dbRender, to.tbRender
	to.mu.RUnlock()

	largest := [2]int{math.MinInt64, math.MinInt64}
	to.idx.Range(func(key, value any) bool {
		if d := key.(int); d > largest[0] {
			largest[0] = d
			if t := value.([]int); len(t) > 0 {
				largest[1] = t[len(t)-1]
			}
		}
		return true
	})

	if largest[0] != math.MinInt64 || largest[1] != math.MinInt64 {
		db, tb, ok = dbRender(largest[0]), tbRender(largest[1]), true
	}

	return
}
