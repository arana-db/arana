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
	// ShardMetadata 分片元数据
	ShardMetadata struct {
		Steps    int           // 步进总数
		Stepper  Stepper       // 范围步进器
		Computer ShardComputer // 分片计算器
	}

	// ShardComputer computes the shard index from an input value.
	ShardComputer interface {
		// Compute computes the shard index.
		Compute(value interface{}) (int, error)
	}
)

// VTable 表示一个虚拟的逻辑表, 一个虚拟逻辑表可能分布散落在多个物理库表上, 虚拟表会维护一个分布拓扑以及分片元数据
type VTable struct {
	topology *Topology
	shards   map[string][2]*ShardMetadata // column -> [db shard metadata,table shard metadata]
}

func (vt *VTable) Topology() *Topology {
	return vt.topology
}

func (vt *VTable) Shard(column string, value interface{}) (db int, table int, err error) {
	sm, ok := vt.shards[column]
	if !ok {
		err = errors.Errorf("no shard metadata for column %s", column)
		return
	}

	if sm[0] != nil {
		if db, err = sm[0].Computer.Compute(value); err != nil {
			return
		}
	}
	if sm[1] != nil {
		if table, err = sm[1].Computer.Compute(value); err != nil {
			return
		}
	}

	return
}

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

func (vt *VTable) SetShardMetadata(column string, dbShardMetadata, tblShardMetadata *ShardMetadata) {
	if vt.shards == nil {
		vt.shards = make(map[string][2]*ShardMetadata)
	}
	vt.shards[column] = [2]*ShardMetadata{dbShardMetadata, tblShardMetadata}
}

func (vt *VTable) SetTopology(topology *Topology) {
	vt.topology = topology
}

// Rule 定义了核心分库分表规则
type Rule struct {
	mu    sync.RWMutex
	vtabs map[string]*VTable // table name -> *VTable
}

func (ru *Rule) HasColumn(table, column string) bool {
	vt, ok := ru.VTable(table)
	if !ok {
		return false
	}
	_, _, ok = vt.GetShardMetadata(column)
	return ok
}

func (ru *Rule) Has(table string) bool {
	ru.mu.RLock()
	_, ok := ru.vtabs[table]
	ru.mu.RUnlock()
	return ok
}

func (ru *Rule) RemoveVTable(table string) {
	ru.mu.Lock()
	delete(ru.vtabs, table)
	ru.mu.Unlock()
}

func (ru *Rule) SetVTable(table string, vt *VTable) {
	ru.mu.Lock()
	if ru.vtabs == nil {
		ru.vtabs = make(map[string]*VTable)
	}
	ru.vtabs[table] = vt
	ru.mu.Unlock()
}

func (ru *Rule) VTable(name string) (*VTable, bool) {
	ru.mu.RLock()
	vt, ok := ru.vtabs[name]
	ru.mu.RUnlock()
	return vt, ok
}

func (ru *Rule) MustVTable(name string) *VTable {
	v, ok := ru.VTable(name)
	if !ok {
		panic(fmt.Sprintf("no such VTable %s!", name))
	}
	return v
}
