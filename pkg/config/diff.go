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
 *
 */

package config

import "reflect"

func (r Rules) Compare(o Rules) bool {
	if len(r) == 0 && len(o) == 0 {
		return true
	}

	if len(r) != len(o) {
		return false
	}

	newT := make([]*Rule, 0, 4)
	updateT := make([]*Rule, 0, 4)
	deleteT := make([]*Rule, 0, 4)

	newTmp := map[string]*Rule{}
	oldTmp := map[string]*Rule{}

	for i := range r {
		newTmp[r[i].Column] = r[i]
	}
	for i := range o {
		oldTmp[o[i].Column] = o[i]
	}

	for i := range r {
		if _, ok := oldTmp[o[i].Column]; !ok {
			newT = append(newT, o[i])
		}
	}

	for i := range o {
		val, ok := newTmp[o[i].Column]
		if !ok {
			deleteT = append(deleteT, o[i])
			continue
		}

		if !reflect.DeepEqual(val, o[i]) {
			updateT = append(updateT, val)
			continue
		}
	}

	return len(newT) == 0 && len(updateT) == 0 && len(deleteT) == 0
}

func (t *Table) Compare(o *Table) bool {
	if len(t.DbRules) != len(o.DbRules) {
		return false
	}

	if len(t.TblRules) != len(o.TblRules) {
		return false
	}

	if !Rules(t.DbRules).Compare(o.DbRules) {
		return false
	}
	if !Rules(t.TblRules).Compare(o.TblRules) {
		return false
	}

	if !reflect.DeepEqual(t.Topology, o.Topology) || !reflect.DeepEqual(t.ShadowTopology, o.ShadowTopology) {
		return false
	}

	if t.AllowFullScan == o.AllowFullScan {
		return false
	}

	if !reflect.DeepEqual(t.Attributes, o.Attributes) {
		return false
	}

	return true
}

func (c Clusters) Diff(old Clusters) *ClustersEvent {
	newC := make([]*DataSourceCluster, 0, 4)
	updateC := make([]*ClusterEvent, 0, 4)
	deleteC := make([]*DataSourceCluster, 0, 4)

	newTmp := map[string]*DataSourceCluster{}
	oldTmp := map[string]*DataSourceCluster{}

	for i := range c {
		newTmp[c[i].Name] = c[i]
	}

	for i := range old {
		oldTmp[old[i].Name] = old[i]
	}

	for i := range c {
		if _, ok := oldTmp[c[i].Name]; !ok {
			newC = append(newC, c[i])
		}
	}

	for i := range old {
		val, ok := newTmp[old[i].Name]
		if !ok {
			deleteC = append(deleteC, old[i])
			continue
		}

		if !reflect.DeepEqual(val, old[i]) {
			updateC = append(updateC, val.Diff(old[i]))
			continue
		}
	}

	return &ClustersEvent{
		AddCluster:    newC,
		UpdateCluster: updateC,
		DeleteCluster: deleteC,
	}
}

func (d *DataSourceCluster) Diff(old *DataSourceCluster) *ClusterEvent {

	ret := &ClusterEvent{
		Name:        d.Name,
		Type:        d.Type,
		SqlMaxLimit: d.SqlMaxLimit,
		Parameters:  d.Parameters,
		GroupsEvent: Groups(d.Groups).Diff(old.Groups),
	}

	return ret
}

func (g Groups) Diff(old Groups) *GroupsEvent {
	newG := make([]*Group, 0, 4)
	updateG := make([]*Group, 0, 4)
	deleteG := make([]*Group, 0, 4)

	newTmp := map[string]*Group{}
	oldTmp := map[string]*Group{}

	for i := range g {
		newTmp[g[i].Name] = g[i]
	}
	for i := range old {
		oldTmp[old[i].Name] = old[i]
	}

	for i := range g {
		if _, ok := oldTmp[g[i].Name]; !ok {
			newG = append(newG, g[i])
		}
	}

	for i := range old {
		val, ok := newTmp[old[i].Name]
		if !ok {
			deleteG = append(deleteG, old[i])
			continue
		}

		if !reflect.DeepEqual(val, old[i]) {
			updateG = append(updateG, val)
			continue
		}
	}

	return &GroupsEvent{
		AddGroups:    newG,
		DeleteGroups: deleteG,
		UpdateGroups: updateG,
	}
}

func (s *ShardingRule) Diff(old *ShardingRule) *ShardingRuleEvent {
	newT := make([]*Table, 0, 4)
	updateT := make([]*Table, 0, 4)
	deleteT := make([]*Table, 0, 4)

	newTmp := map[string]*Table{}
	oldTmp := map[string]*Table{}

	for i := range s.Tables {
		newTmp[s.Tables[i].Name] = s.Tables[i]
	}
	for i := range old.Tables {
		oldTmp[old.Tables[i].Name] = old.Tables[i]
	}

	for i := range s.Tables {
		if _, ok := oldTmp[s.Tables[i].Name]; !ok {
			newT = append(newT, s.Tables[i])
		}
	}

	for i := range old.Tables {
		val, ok := newTmp[old.Tables[i].Name]
		if !ok {
			deleteT = append(deleteT, old.Tables[i])
			continue
		}

		if !reflect.DeepEqual(val, old.Tables[i]) {
			updateT = append(updateT, val)
			continue
		}
	}

	return &ShardingRuleEvent{
		AddTables:    newT,
		UpdateTables: updateT,
		DeleteTables: deleteT,
	}
}
