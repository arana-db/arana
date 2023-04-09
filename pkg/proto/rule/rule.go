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

//go:generate mockgen -destination=../../../testdata/mock_rule.go -package=testdata . ShardComputer
package rule

import (
	"encoding/json"
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

type (
	// RawShardRule represents the raw database_rule and table_rule of shards.
	RawShardRule struct {
		Column string `json:"column"`
		Type   string `json:"type"`
		Expr   string `json:"expr"`
		Step   int    `json:"step,omitempty"`
	}
	// RawShardMetadata represents the raw metadata of shards.
	RawShardMetadata struct {
		Name         string                 `json:"name,omitempty"`
		SequenceType string                 `json:"sequence_type,omitempty"`
		DbRules      []*RawShardRule        `json:"db_rules"`
		TblRules     []*RawShardRule        `json:"tbl_rules"`
		Attributes   map[string]interface{} `json:"attributes,omitempty"`
	}
)

func (r RawShardMetadata) JSONMarshal() (string, error) {
	jsons, errs := json.Marshal(r)
	if errs != nil {
		return "", errors.Errorf("cannot marshal the shard metadata to json")
	}
	return string(jsons), nil
}

// VTable represents a virtual/logical table.
type VTable struct {
	attributes
	name          string // TODO: set name
	autoIncrement *AutoIncrement
	topology      *Topology
	shards        map[string][2]*ShardMetadata // column -> [db shard metadata,table shard metadata]
	shardsC       []*VShards                   // todo use shardsC replace shards
	rawShards     *RawShardMetadata
}

type VShards struct {
	columns       []string
	shardMetadata [2]*ShardMetadata
	key           string
}

func (vt *VShards) HasColumns(columns []string) []int {
	var bingoList []int
	for _, v := range vt.columns {
		for i, column := range columns {
			if v != column {
				return []int{}
			}
			bingoList = append(bingoList, i)
		}
	}
	return bingoList
}

func (vt *VTable) HasColumn(column string) bool {
	_, ok := vt.shards[column]
	return ok
}

func (vt *VTable) Name() string {
	return vt.name
}

func (vt *VTable) GetAutoIncrement() *AutoIncrement {
	return vt.autoIncrement
}

func (vt *VTable) SetAutoIncrement(seq *AutoIncrement) {
	vt.autoIncrement = seq
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
func (vt *VTable) SetRawShardMetaData(rawData *RawShardMetadata) {
	vt.rawShards = rawData
}

func (vt *VTable) GetShardMetaDataJSON() (map[string]string, error) {
	res := make(map[string]string)
	var (
		val []byte
		err error
	)

	res["name"] = vt.rawShards.Name
	res["sequence_type"] = vt.rawShards.SequenceType

	val, err = json.Marshal(vt.rawShards.DbRules)
	if err != nil {
		return res, err
	}
	res["db_rules"] = string(val)

	val, err = json.Marshal(vt.rawShards.TblRules)
	if err != nil {
		return res, err
	}
	res["tbl_rules"] = string(val)

	val, err = json.Marshal(vt.rawShards.Attributes)
	if err != nil {
		return res, err
	}
	res["attributes"] = string(val)

	return res, nil
}

// Topology returns the topology of VTable.
func (vt *VTable) Topology() *Topology {
	return vt.topology
}

// Shard returns the shard result.
func (vt *VTable) Shard(column string, value interface{}) (uint32 /* db */, uint32 /* table */, error) {
	var (
		db, table int
		err       error
	)
	sm, ok := vt.shards[column]
	if !ok {
		return 0, 0, errors.Errorf("no shard metadata for column %s", column)
	}

	if sm[0] != nil { // compute the index of db
		if db, err = sm[0].Computer.Compute(value); err != nil {
			return 0, 0, errors.WithStack(err)
		}
	}
	if sm[1] != nil { // compute the index of table
		if table, err = sm[1].Computer.Compute(value); err != nil {
			return 0, 0, errors.WithStack(err)
		}
	}

	return uint32(db), uint32(table), nil
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

func (vt *VTable) SetName(name string) {
	vt.name = name
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
func (ru *Rule) RemoveVTable(table string) bool {
	ru.mu.Lock()
	_, ok := ru.vtabs[table]
	if ok {
		delete(ru.vtabs, table)
	}
	ru.mu.Unlock()
	return ok
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

// VTables returns all the VTable
func (ru *Rule) VTables() map[string]*VTable {
	ru.mu.RLock()
	defer ru.mu.RUnlock()
	return ru.vtabs
}

// DBRule returns all the database rule
func (ru *Rule) DBRule() map[string][]*RawShardRule {
	ru.mu.RLock()
	defer ru.mu.RUnlock()
	allDBRule := make(map[string][]*RawShardRule, 10)
	for _, tb := range ru.vtabs {
		allDBRule[tb.name] = tb.rawShards.DbRules
	}
	return allDBRule
}

// MustVTable returns the VTable with given table name, panic if not exist.
func (ru *Rule) MustVTable(name string) *VTable {
	v, ok := ru.VTable(name)
	if !ok {
		panic(fmt.Sprintf("no such VTable %s!", name))
	}
	return v
}

// Range ranges each VTable
func (ru *Rule) Range(f func(table string, vt *VTable) bool) {
	ru.mu.RLock()
	defer ru.mu.RUnlock()

	for k, v := range ru.vtabs {
		if !f(k, v) {
			break
		}
	}
}

func (vt *VTable) GetShardColumnIndex(columns []string) (bingoList []int) {
	for _, v := range vt.shardsC {
		if bingoList = v.HasColumns(columns); len(bingoList) > 0 {
			return bingoList
		}
	}
	return []int{}
}
