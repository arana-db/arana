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
	addTenants := make([]string, 0, 4)
	deleteTenants := make([]string, 0, 4)

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
			addTenants = append(addTenants, i)
		}
	}

	for i := range oldTmp {
		if _, ok := newTmp[i]; !ok {
			deleteTenants = append(deleteTenants, i)
		}
	}

	return &TenantsEvent{
		AddTenants:    addTenants,
		DeleteTenants: deleteTenants,
	}
}

func (n Nodes) Diff(old Nodes) *NodesEvent {
	addNodes := make([]*Node, 0, 4)
	updateNodes := make([]*Node, 0, 4)
	deleteNodes := make([]*Node, 0, 4)

	for i := range n {
		if _, ok := old[i]; !ok {
			addNodes = append(addNodes, n[i])
		}
	}

	for i := range old {
		val, ok := n[old[i].Name]
		if !ok {
			deleteNodes = append(deleteNodes, old[i])
			continue
		}

		if !val.Equals(old[i]) {
			updateNodes = append(updateNodes, val)
			continue
		}
	}

	return &NodesEvent{
		AddNodes:    addNodes,
		UpdateNodes: updateNodes,
		DeleteNodes: deleteNodes,
	}
}

func (u Users) Diff(old Users) *UsersEvent {
	addUsers := make([]*User, 0, 4)
	updateUsers := make([]*User, 0, 4)
	deleteUsers := make([]*User, 0, 4)

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
			addUsers = append(addUsers, newTmp[i])
		}
	}

	for i := range oldTmp {
		val, ok := newTmp[oldTmp[i].Username]
		if !ok {
			deleteUsers = append(deleteUsers, oldTmp[i])
			continue
		}

		if !val.Equals(oldTmp[i]) {
			updateUsers = append(updateUsers, val)
			continue
		}
	}

	return &UsersEvent{
		AddUsers:    addUsers,
		UpdateUsers: updateUsers,
		DeleteUsers: deleteUsers,
	}
}

func (c Clusters) Diff(old Clusters) *ClustersEvent {
	addClusters := make([]*DataSourceCluster, 0, 4)
	updateClusters := make([]*ClusterEvent, 0, 4)
	deleteClusters := make([]*DataSourceCluster, 0, 4)

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
			addClusters = append(addClusters, c[i])
		}
	}

	for i := range old {
		val, ok := newTmp[old[i].Name]
		if !ok {
			deleteClusters = append(deleteClusters, old[i])
			continue
		}

		if !reflect.DeepEqual(val, old[i]) {
			updateClusters = append(updateClusters, val.Diff(old[i]))
			continue
		}
	}

	return &ClustersEvent{
		AddCluster:    addClusters,
		UpdateCluster: updateClusters,
		DeleteCluster: deleteClusters,
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
	addGroups := make([]*Group, 0, 4)
	updateGroups := make([]*Group, 0, 4)
	deleteGroups := make([]*Group, 0, 4)

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
			addGroups = append(addGroups, g[i])
		}
	}

	for i := range old {
		val, ok := newTmp[old[i].Name]
		if !ok {
			deleteGroups = append(deleteGroups, old[i])
			continue
		}

		if !reflect.DeepEqual(val, old[i]) {
			updateGroups = append(updateGroups, val)
			continue
		}
	}

	return &GroupsEvent{
		AddGroups:    addGroups,
		DeleteGroups: deleteGroups,
		UpdateGroups: updateGroups,
	}
}

func (s *ShardingRule) Diff(old *ShardingRule) *ShardingRuleEvent {
	addTables := make([]*Table, 0, 4)
	updateTables := make([]*Table, 0, 4)
	deleteTables := make([]*Table, 0, 4)

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
			addTables = append(addTables, s.Tables[i])
		}
	}

	for i := range old.Tables {
		val, ok := newTmp[old.Tables[i].Name]
		if !ok {
			deleteTables = append(deleteTables, old.Tables[i])
			continue
		}

		if !reflect.DeepEqual(val, old.Tables[i]) {
			updateTables = append(updateTables, val)
			continue
		}
	}

	return &ShardingRuleEvent{
		AddTables:    addTables,
		UpdateTables: updateTables,
		DeleteTables: deleteTables,
	}
}

func (s *ShadowRule) Diff(old *ShadowRule) *ShadowRuleEvent {
	addTables := make([]*ShadowTable, 0, 4)
	updateTables := make([]*ShadowTable, 0, 4)
	deleteTables := make([]*ShadowTable, 0, 4)

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
			addTables = append(addTables, s.ShadowTables[i])
		}
	}

	for i := range old.ShadowTables {
		val, ok := newTmp[old.ShadowTables[i].Name]
		if !ok {
			deleteTables = append(deleteTables, old.ShadowTables[i])
			continue
		}

		if !reflect.DeepEqual(val, old.ShadowTables[i]) {
			updateTables = append(updateTables, val)
			continue
		}
	}

	return &ShadowRuleEvent{
		AddTables:    addTables,
		UpdateTables: updateTables,
		DeleteTables: deleteTables,
	}
}
