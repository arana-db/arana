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

import (
	"github.com/arana-db/arana/pkg/proto"
)

const (
	attrAllowFullScan byte = 0x01
)

type (
	// ShardColumn represents the shard column.
	ShardColumn struct {
		Name    string
		Steps   int
		Stepper Stepper
	}

	// ShardMetadata represents the metadata of shards.
	ShardMetadata struct {
		ShardColumns []*ShardColumn
		Computer     ShardComputer // compute shards
	}

	// ShardComputer computes the shard index from an input value.
	ShardComputer interface {
		// Variables returns the variable names.
		Variables() []string
		// Compute computes the shard index.
		Compute(values ...proto.Value) (int, error)
	}

	VShard struct {
		sync.Once
		DB, Table *ShardMetadata
		variables []string
	}

	ShardComputerFactory interface {
		Apply(columns []string, expr string) (ShardComputer, error)
	}
)

var _shardComputers map[string]ShardComputerFactory

type FuncShardComputerFactory func([]string, string) (ShardComputer, error)

func (f FuncShardComputerFactory) Apply(columns []string, expr string) (ShardComputer, error) {
	return f(columns, expr)
}

func RegisterShardComputer(typ string, factory ShardComputerFactory) {
	if _shardComputers == nil {
		_shardComputers = make(map[string]ShardComputerFactory)
	}
	_shardComputers[typ] = factory
}

func NewComputer(typ string, columns []string, expr string) (ShardComputer, error) {
	f, ok := _shardComputers[typ]
	if !ok {
		return nil, errors.Errorf("no such shard computer type '%s'", typ)
	}
	return f.Apply(columns, expr)
}

func (sm *ShardMetadata) GetShardColumn(name string) *ShardColumn {
	for i := range sm.ShardColumns {
		if sm.ShardColumns[i].Name == name {
			return sm.ShardColumns[i]
		}
	}
	return nil
}

func (vs *VShard) Len() int {
	return len(vs.Variables())
}

func (vs *VShard) Variables() []string {
	vs.Do(func() {
		visits := make(map[string]struct{})

		handle := func(vars []string) {
			for i := range vars {
				if _, ok := visits[vars[i]]; ok {
					continue
				}
				visits[vars[i]] = struct{}{}
				vs.variables = append(vs.variables, vars[i])
			}
		}
		if vs.DB != nil {
			handle(vs.DB.Computer.Variables())
		}
		if vs.Table != nil {
			handle(vs.Table.Computer.Variables())
		}
	})
	return vs.variables
}

// VTable represents a virtual/logical table.
type VTable struct {
	attributes
	name          string // TODO: set name
	autoIncrement *AutoIncrement
	topology      *Topology
	shards        []*VShard
	ext           map[string]interface{}
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

func (vt *VTable) HasVShard(keys ...string) bool {
	_, ok := vt.SearchVShard(keys...)
	return ok
}

func (vt *VTable) SearchVShard(keys ...string) (*VShard, bool) {
	vShards := vt.GetVShards()
L:
	for i := range vShards {
		variables := vShards[i].Variables()
		if len(variables) != len(keys) {
			continue
		}
		for j := range keys {
			if variables[j] != keys[j] {
				continue L
			}
		}
		return vShards[i], true
	}
	return nil, false
}

func (vt *VTable) GetVShards() []*VShard {
	return vt.shards
}

func (vt *VTable) GetShardMetaDataJSON() (map[string]string, error) {
	res := make(map[string]string)

	res["name"] = vt.Name()
	res["sequence_type"] = vt.GetAutoIncrement().Type

	var (
		dbRules  []interface{}
		tblRules []interface{}
	)
	for _, vs := range vt.GetVShards() {
		dbShardColumns := make([]string, 0, len(vs.DB.ShardColumns))
		for i := range vs.DB.ShardColumns {
			dbShardColumns = append(dbShardColumns, vs.DB.ShardColumns[i].Name)
		}
		tblShardColumns := make([]string, 0, len(vs.Table.ShardColumns))
		for i := range vs.Table.ShardColumns {
			tblShardColumns = append(tblShardColumns, vs.Table.ShardColumns[i].Name)
		}

		dbRules = append(dbRules, map[string]interface{}{
			"columns":    dbShardColumns,
			"expression": fmt.Sprintf("%s", vs.DB.Computer),
		})
		tblRules = append(tblRules, map[string]interface{}{
			"columns":    tblShardColumns,
			"expression": fmt.Sprintf("%s", vs.Table.Computer),
		})
	}

	dbRulesJson, _ := json.Marshal(dbRules)
	tblRulesJson, _ := json.Marshal(tblRules)

	res["db_rules"] = string(dbRulesJson)
	res["tbl_rules"] = string(tblRulesJson)

	b, _ := json.Marshal(vt.ext)
	res["attributes"] = string(b)

	return res, nil
}

// Topology returns the topology of VTable.
func (vt *VTable) Topology() *Topology {
	return vt.topology
}

// Shard returns the shard result.
func (vt *VTable) Shard(inputs map[string]proto.Value) (uint32 /* db */, uint32 /* table */, error) {
	var bingo *VShard
L:
	for i := range vt.shards {
		for _, key := range vt.shards[i].Variables() {
			if _, ok := inputs[key]; !ok {
				continue L
			}
		}
		if bingo != nil {
			return 0, 0, errors.New("conflict vshards")
		}
		bingo = vt.shards[i]
	}

	if bingo == nil {
		return 0, 0, errors.Errorf("no available vshards")
	}

	compute := func(c ShardComputer) (int, error) {
		args := make([]proto.Value, 0, len(c.Variables()))
		for _, variable := range c.Variables() {
			args = append(args, inputs[variable])
		}
		return c.Compute(args...)
	}

	var (
		db, table int
		err       error
	)

	if bingo.DB != nil {
		if db, err = compute(bingo.DB.Computer); err != nil {
			return 0, 0, errors.Wrap(err, "cannot compute db shard")
		}
	}

	if bingo.Table != nil {
		if table, err = compute(bingo.Table.Computer); err != nil {
			return 0, 0, errors.Wrap(err, "cannot compute table shard")
		}
	}

	return uint32(db), uint32(table), nil
}

func (vt *VTable) AddVShards(shard *VShard) {
	vt.shards = append(vt.shards, shard)
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

// GetInvertedPhyTableMap returns the inverse relationship from physical table name to logical table name.
func (ru *Rule) GetInvertedPhyTableMap() map[string]string {
	var invertedIndex map[string]string
	for logicalTable, v := range ru.VTables() {
		t := v.Topology()
		t.Each(func(x, y int) bool {
			if _, phyTable, ok := t.Render(x, y); ok {
				if invertedIndex == nil {
					invertedIndex = make(map[string]string)
				}
				invertedIndex[phyTable] = logicalTable
			}
			return true
		})
	}

	return invertedIndex
}

// GetInvertedVTableMap returns the inverse relationship from logical table name to all physical table name.
// TODO: did we need an sorted version?
func (ru *Rule) GetInvertedVTableMap() map[string][]string {
	var invertedIndex map[string][]string

	vtables := ru.VTables()
	if len(vtables) > 0 {
		invertedIndex = make(map[string][]string)
	}

	for logicalTable, v := range vtables {
		t := v.Topology()

		_, tblLength := t.Len()
		// memory allocation
		invertedIndex[logicalTable] = make([]string, 0, tblLength)

		t.Each(func(x, y int) bool {
			if _, phyTable, ok := t.Render(x, y); ok {
				invertedIndex[logicalTable] = append(invertedIndex[logicalTable], phyTable)
			}
			return true
		})
	}

	return invertedIndex
}
