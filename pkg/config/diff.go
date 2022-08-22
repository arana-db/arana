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

import (
	"reflect"
)

func (t Tenants) Diff(old Tenants) *TenantsEvent {
	newN := make([]string, 0, 4)
	deleteN := make([]string, 0, 4)

	newTmp := map[string]struct{}{}
	oldTmp := map[string]struct{}{}

	for i := range t {
		newTmp[t[i]] = struct{}{}
	}

	for i := range old {
		oldTmp[old[i]] = struct{}{}
	}

	for i := range newTmp {
		if _, ok := oldTmp[i]; !ok {
			newN = append(newN, i)
		}
	}

	for i := range oldTmp {
		if _, ok := newTmp[i]; !ok {
			deleteN = append(deleteN, i)
		}
	}

	return &TenantsEvent{
		AddTenants:    newN,
		DeleteTenants: deleteN,
	}
}

func (n Nodes) Diff(old Nodes) *NodesEvent {
	newN := make([]*Node, 0, 4)
	updateN := make([]*Node, 0, 4)
	deleteN := make([]*Node, 0, 4)

	for i := range n {
		if _, ok := old[i]; !ok {
			newN = append(newN, n[i])
		}
	}

	for i := range old {
		val, ok := n[old[i].Name]
		if !ok {
			deleteN = append(deleteN, old[i])
			continue
		}

		if !val.Compare(old[i]) {
			updateN = append(updateN, val)
			continue
		}
	}

	return &NodesEvent{
		AddNodes:    newN,
		UpdateNodes: updateN,
		DeleteNodes: deleteN,
	}
}

func (u Users) Diff(old Users) *UsersEvent {
	newN := make([]*User, 0, 4)
	updateN := make([]*User, 0, 4)
	deleteN := make([]*User, 0, 4)

	newTmp := map[string]*User{}
	oldTmp := map[string]*User{}

	for i := range u {
		newTmp[u[i].Username] = u[i]
	}

	for i := range old {
		oldTmp[old[i].Username] = old[i]
	}

	for i := range newTmp {
		if _, ok := oldTmp[i]; !ok {
			newN = append(newN, newTmp[i])
		}
	}

	for i := range oldTmp {
		val, ok := newTmp[oldTmp[i].Username]
		if !ok {
			deleteN = append(deleteN, oldTmp[i])
			continue
		}

		if !val.Compare(oldTmp[i]) {
			updateN = append(updateN, val)
			continue
		}
	}

	return &UsersEvent{
		AddUsers:    newN,
		UpdateUsers: updateN,
		DeleteUsers: deleteN,
	}
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

func (s *ShadowRule) Diff(old *ShadowRule) *ShadowRuleEvent {
	newT := make([]*ShadowTable, 0, 4)
	updateT := make([]*ShadowTable, 0, 4)
	deleteT := make([]*ShadowTable, 0, 4)

	newTmp := map[string]*ShadowTable{}
	oldTmp := map[string]*ShadowTable{}

	for i := range s.ShadowTables {
		newTmp[s.ShadowTables[i].Name] = s.ShadowTables[i]
	}
	for i := range old.ShadowTables {
		oldTmp[old.ShadowTables[i].Name] = old.ShadowTables[i]
	}

	for i := range s.ShadowTables {
		if _, ok := oldTmp[s.ShadowTables[i].Name]; !ok {
			newT = append(newT, s.ShadowTables[i])
		}
	}

	for i := range old.ShadowTables {
		val, ok := newTmp[old.ShadowTables[i].Name]
		if !ok {
			deleteT = append(deleteT, old.ShadowTables[i])
			continue
		}

		if !reflect.DeepEqual(val, old.ShadowTables[i]) {
			updateT = append(updateT, val)
			continue
		}
	}

	return &ShadowRuleEvent{
		AddTables:    newT,
		UpdateTables: updateT,
		DeleteTables: deleteT,
	}
}
