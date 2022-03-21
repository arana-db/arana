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

//go:generate mockgen -destination=../../../testdata/mock_rule.go -package=testdata . ShardComputer
package rule

import (
	"fmt"
	"sync"
)

import (
	"github.com/pkg/errors"
)

type (
	// ShardMetadata represents the metadata of shards.
	ShardMetadata struct {
		Steps    int           // steps
		Stepper  Stepper       // stepper
		Computer ShardComputer // compute shards
	}

	// ShardComputer computes the shard index from an input value.
	ShardComputer interface {
		// Compute computes the shard index.
		Compute(value interface{}) (int, error)
	}

	DirectShardComputer func(interface{}) (int, error)
)

func (d DirectShardComputer) Compute(value interface{}) (int, error) {
	return d(value)
}

const (
	attrAllowFullScan byte = 0x01
)

// VTable represents a virtual/logical table.
type VTable struct {
	attributes
	topology *Topology
	shards   map[string][2]*ShardMetadata // column -> [db shard metadata,table shard metadata]
}

func (vt *VTable) SetAllowFullScan(allow bool) {
	vt.setAttributeBool(attrAllowFullScan, allow)
}

func (vt *VTable) AllowFullScan() bool {
	ret, _ := vt.attributeBool(attrAllowFullScan)
	return ret
}

func (vt *VTable) GetShardKeys() []string {
	keys := make([]string, 0, len(vt.shards))
	for k := range vt.shards {
		keys = append(keys, k)
	}
	return keys
}

// Topology returns the topology of VTable.
func (vt *VTable) Topology() *Topology {
	return vt.topology
}

// Shard returns the shard result.
func (vt *VTable) Shard(column string, value interface{}) (db int, table int, err error) {
	sm, ok := vt.shards[column]
	if !ok {
		err = errors.Errorf("no shard metadata for column %s", column)
		return
	}

	if sm[0] != nil { // compute the index of db
		if db, err = sm[0].Computer.Compute(value); err != nil {
			return
		}
	}
	if sm[1] != nil { // compute the index of table
		if table, err = sm[1].Computer.Compute(value); err != nil {
			return
		}
	}

	return
}

// GetShardMetadata returns the shard metadata with given column.
func (vt *VTable) GetShardMetadata(column string) (db *ShardMetadata, tbl *ShardMetadata, ok bool) {
	var exist [2]*ShardMetadata
	if exist, ok = vt.shards[column]; !ok {
		return
	}
	db, tbl = exist[0], exist[1]
	x, y := vt.Topology().Len()

	// fix steps
	if db != nil && db.Steps == 0 {
		db.Steps = x
	}
	if tbl != nil && tbl.Steps == 0 {
		tbl.Steps = y
	}

	return
}

// SetShardMetadata sets the shard metadata.
func (vt *VTable) SetShardMetadata(column string, dbShardMetadata, tblShardMetadata *ShardMetadata) {
	if vt.shards == nil {
		vt.shards = make(map[string][2]*ShardMetadata)
	}
	vt.shards[column] = [2]*ShardMetadata{dbShardMetadata, tblShardMetadata}
}

// SetTopology sets the topology.
func (vt *VTable) SetTopology(topology *Topology) {
	vt.topology = topology
}

// Rule represents sharding rule, a Rule contains multiple logical tables.
type Rule struct {
	mu    sync.RWMutex
	vtabs map[string]*VTable // table name -> *VTable
}

// HasColumn returns true if the table and columns exists.
func (ru *Rule) HasColumn(table, column string) bool {
	vt, ok := ru.VTable(table)
	if !ok {
		return false
	}
	_, _, ok = vt.GetShardMetadata(column)
	return ok
}

// Has return true if the table exists.
func (ru *Rule) Has(table string) bool {
	if ru == nil {
		return false
	}
	ru.mu.RLock()
	_, ok := ru.vtabs[table]
	ru.mu.RUnlock()
	return ok
}

// RemoveVTable removes the VTable with given table.
func (ru *Rule) RemoveVTable(table string) {
	ru.mu.Lock()
	delete(ru.vtabs, table)
	ru.mu.Unlock()
}

// SetVTable sets a VTable.
func (ru *Rule) SetVTable(table string, vt *VTable) {
	ru.mu.Lock()
	if ru.vtabs == nil {
		ru.vtabs = make(map[string]*VTable)
	}
	ru.vtabs[table] = vt
	ru.mu.Unlock()
}

// VTable returns the VTable with given table name.
func (ru *Rule) VTable(table string) (*VTable, bool) {
	ru.mu.RLock()
	vt, ok := ru.vtabs[table]
	ru.mu.RUnlock()
	return vt, ok
}

// MustVTable returns the VTable with given table name, panic if not exist.
func (ru *Rule) MustVTable(name string) *VTable {
	v, ok := ru.VTable(name)
	if !ok {
		panic(fmt.Sprintf("no such VTable %s!", name))
	}
	return v
}
