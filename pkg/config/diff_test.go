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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestNodes_Diff(t *testing.T) {
	type args struct {
		old Nodes
	}
	tests := []struct {
		name string
		n    Nodes
		args args
		want *NodesEvent
	}{
		{
			name: "NotChange_Nodes",
			n: map[string]*Node{
				"mock_node_1": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
				},
			},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes:    []*Node{},
				UpdateNodes: []*Node{},
				DeleteNodes: []*Node{},
			},
		},
		{
			name: "Change_AddNodes",
			n: map[string]*Node{
				"mock_node_1": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
				},
				"mock_node_2": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
				},
			},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes: []*Node{
					{
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
				UpdateNodes: []*Node{},
				DeleteNodes: []*Node{},
			},
		},
		{
			name: "Change_DeleteNodes",
			n:    map[string]*Node{},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes:    []*Node{},
				UpdateNodes: []*Node{},
				DeleteNodes: []*Node{
					{
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
		},

		{
			name: "Change_UpdateNodes",
			n: map[string]*Node{
				"mock_node_1": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
					Parameters: map[string]string{
						"mock_param_key_1": "mock_param_value_1",
					},
				},
			},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes: []*Node{},
				UpdateNodes: []*Node{
					{
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
						Parameters: map[string]string{
							"mock_param_key_1": "mock_param_value_1",
						},
					},
				},
				DeleteNodes: []*Node{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.n.Diff(tt.args.old), "Diff(%v)", tt.args.old)
		})
	}
}

func TestTenants_Diff(t *testing.T) {
	tests := []struct {
		name string
		t    Tenants
		old  Tenants
		want *TenantsEvent
	}{
		{
			name: "Test case 1: No changes",
			t:    Tenants{"tenant1", "tenant2"},
			old:  Tenants{"tenant1", "tenant2"},
			want: &TenantsEvent{
				AddTenants:    []string{},
				DeleteTenants: []string{},
			},
		},
		{
			name: "Test case 2: Add new tenant",
			t:    Tenants{"tenant1", "tenant2", "tenant3"},
			old:  Tenants{"tenant1", "tenant2"},
			want: &TenantsEvent{
				AddTenants:    []string{"tenant3"},
				DeleteTenants: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.Diff(tt.old); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tenants.Diff() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUsers_Diff(t *testing.T) {
	tests := []struct {
		name string
		u    Users
		old  Users
		want *UsersEvent
	}{
		{
			name: "Test case 1: No changes",
			u:    Users{&User{Username: "user1", Password: "password1"}, &User{Username: "user2", Password: "password2"}},
			old:  Users{&User{Username: "user1", Password: "password1"}, &User{Username: "user2", Password: "password2"}},
			want: &UsersEvent{
				AddUsers:    []*User{},
				UpdateUsers: []*User{},
				DeleteUsers: []*User{},
			},
		},
		{
			name: "Test case 2: Add new user",
			u:    Users{&User{Username: "user1", Password: "password1"}, &User{Username: "user2", Password: "password2"}, &User{Username: "user3", Password: "password3"}},
			old:  Users{&User{Username: "user1", Password: "password1"}, &User{Username: "user2", Password: "password2"}},
			want: &UsersEvent{
				AddUsers:    []*User{{Username: "user3", Password: "password3"}},
				UpdateUsers: []*User{},
				DeleteUsers: []*User{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.u.Diff(tt.old); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Users.Diff() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClustersDiff(t *testing.T) {
	// 创建Clusters实例，分别代表旧的和新的状态
	oldClusters := Clusters{
		&DataSourceCluster{
			Name:        "cluster1",
			Type:        "type1",
			SqlMaxLimit: 100,
			Parameters:  ParametersMap{"param1": "value1"},
			Groups:      []*Group{{Name: "group1", Nodes: []string{"node1"}}},
		},
	}

	newClusters := Clusters{
		&DataSourceCluster{
			Name:        "cluster1",
			Type:        "type1",
			SqlMaxLimit: 200,
			Parameters:  ParametersMap{"param1": "value1"},
			Groups:      []*Group{{Name: "group1", Nodes: []string{"node1", "node2"}}}, // 添加了一个新的节点
		},
		&DataSourceCluster{
			Name:        "cluster2",
			Type:        "type2",
			SqlMaxLimit: 300,
			Parameters:  ParametersMap{"param2": "value2"},
			Groups:      []*Group{{Name: "group2", Nodes: []string{"node3"}}},
		},
	}

	event := newClusters.Diff(oldClusters)

	if len(event.AddClusters) != 1 || len(event.DeleteClusters) != 0 || len(event.UpdateClusters) != 1 {
		t.Errorf("ClustersEvent is not correct")
	}

	addedCluster := event.AddClusters[0]
	if addedCluster.Name != "cluster2" || addedCluster.Type != "type2" || addedCluster.SqlMaxLimit != 300 || !reflect.DeepEqual(addedCluster.Parameters, ParametersMap{"param2": "value2"}) {
		t.Errorf("Added Cluster is not correct")
	}

	updatedCluster := event.UpdateClusters[0]
	if updatedCluster.Name != "cluster1" || updatedCluster.Type != "type1" || updatedCluster.SqlMaxLimit != 200 || !reflect.DeepEqual(updatedCluster.Parameters, ParametersMap{"param1": "value1"}) {
		t.Errorf("Updated Cluster is not correct")
	}
}

func TestDataSourceClusterDiff(t *testing.T) {
	oldCluster := &DataSourceCluster{
		Name:        "cluster1",
		Type:        "type1",
		SqlMaxLimit: 100,
		Parameters:  ParametersMap{"param1": "value1"},
		Groups:      []*Group{{Name: "group1", Nodes: []string{"node1"}}},
	}

	newCluster := &DataSourceCluster{
		Name:        "cluster1",
		Type:        "type1",
		SqlMaxLimit: 100,
		Parameters:  ParametersMap{"param1": "value1"},
		Groups:      []*Group{{Name: "group1", Nodes: []string{"node1", "node2"}}}, // 添加了一个新的节点
	}

	event := newCluster.Diff(oldCluster)

	if event.Name != "cluster1" || event.Type != "type1" || event.SqlMaxLimit != 100 || !reflect.DeepEqual(event.Parameters, ParametersMap{"param1": "value1"}) {
		t.Errorf("ClusterEvent is not correct")
	}

	if len(event.GroupsEvent.AddGroups) != 0 || len(event.GroupsEvent.DeleteGroups) != 0 || len(event.GroupsEvent.UpdateGroups) != 1 || event.GroupsEvent.UpdateGroups[0].Name != "group1" {
		t.Errorf("GroupsEvent is not correct")
	}
}

func TestGroupsDiff(t *testing.T) {
	oldGroups := Groups{
		&Group{
			Name:  "group1",
			Nodes: []string{"node1", "node2"},
		},
		&Group{
			Name:  "group2",
			Nodes: []string{"node3", "node4"},
		},
	}

	newGroups := Groups{
		&Group{
			Name:  "group1",
			Nodes: []string{"node1", "node2"},
		},
		&Group{
			Name:  "group3",
			Nodes: []string{"node5", "node6"},
		},
	}

	event := newGroups.Diff(oldGroups)

	if len(event.AddGroups) != 1 || event.AddGroups[0].Name != "group3" {
		t.Errorf("AddGroups is not correct")
	}

	if len(event.DeleteGroups) != 1 || event.DeleteGroups[0].Name != "group2" {
		t.Errorf("DeleteGroups is not correct")
	}

	if len(event.UpdateGroups) != 0 {
		t.Errorf("UpdateGroups is not correct")
	}
}

func TestShardingRuleDiff(t *testing.T) {
	oldRule := &ShardingRule{
		Tables: []*Table{
			{
				Name: "table1",
				DbRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column1"}},
						Type:    "type1",
						Expr:    "expr1",
					},
				},
				TblRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column2"}},
						Type:    "type2",
						Expr:    "expr2",
					},
				},
				Topology: &Topology{
					DbPattern:  "db_pattern1",
					TblPattern: "tbl_pattern1",
				},
				Attributes: map[string]string{"attr1": "value1"},
			},
			{
				Name: "table2",
				DbRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column3"}},
						Type:    "type3",
						Expr:    "expr3",
					},
				},
				TblRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column4"}},
						Type:    "type4",
						Expr:    "expr4",
					},
				},
				Topology: &Topology{
					DbPattern:  "db_pattern2",
					TblPattern: "tbl_pattern2",
				},
				Attributes: map[string]string{"attr2": "value2"},
			},
		},
	}

	newRule := &ShardingRule{
		Tables: []*Table{
			{
				Name: "table1",
				DbRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column1"}},
						Type:    "type1",
						Expr:    "expr1",
					},
				},
				TblRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column2"}},
						Type:    "type2",
						Expr:    "expr2",
					},
				},
				Topology: &Topology{
					DbPattern:  "db_pattern1",
					TblPattern: "tbl_pattern1",
				},
				Attributes: map[string]string{"attr1": "value1"},
			},
			{
				Name: "table3",
				DbRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column5"}},
						Type:    "type5",
						Expr:    "expr5",
					},
				},
				TblRules: []*Rule{
					{
						Columns: []*ColumnRule{{Name: "column6"}},
						Type:    "type6",
						Expr:    "expr6",
					},
				},
				Topology: &Topology{
					DbPattern:  "db_pattern3",
					TblPattern: "tbl_pattern3",
				},
				Attributes: map[string]string{"attr3": "value3"},
			},
		},
	}

	event := newRule.Diff(oldRule)

	if len(event.AddTables) != 1 || event.AddTables[0].Name != "table3" {
		t.Errorf("AddTables is not correct")
	}

	if len(event.DeleteTables) != 1 || event.DeleteTables[0].Name != "table2" {
		t.Errorf("DeleteTables is not correct")
	}

	if len(event.UpdateTables) != 0 {
		t.Errorf("UpdateTables is not correct")
	}
}

func TestShadowRuleDiff(t *testing.T) {
	oldRule := &ShadowRule{
		ShadowTables: []*ShadowTable{
			{
				Name:      "table1",
				Enable:    true,
				GroupNode: "groupNode1",
				MatchRules: []*MatchRule{
					{
						Operation: []string{"op1"},
						MatchType: "type1",
						Attributes: []*RuleAttribute{
							{
								Column: "column1",
								Value:  "value1",
							},
						},
					},
				},
			},
			{
				Name:      "table2",
				Enable:    true,
				GroupNode: "groupNode2",
				MatchRules: []*MatchRule{
					{
						Operation: []string{"op2"},
						MatchType: "type2",
						Attributes: []*RuleAttribute{
							{
								Column: "column2",
								Value:  "value2",
							},
						},
					},
				},
			},
		},
	}

	newRule := &ShadowRule{
		ShadowTables: []*ShadowTable{
			{
				Name:      "table1",
				Enable:    true,
				GroupNode: "groupNode1",
				MatchRules: []*MatchRule{
					{
						Operation: []string{"op1"},
						MatchType: "type1",
						Attributes: []*RuleAttribute{
							{
								Column: "column1",
								Value:  "value1",
							},
						},
					},
				},
			},
			{
				Name:      "table3",
				Enable:    true,
				GroupNode: "groupNode3",
				MatchRules: []*MatchRule{
					{
						Operation: []string{"op3"},
						MatchType: "type3",
						Attributes: []*RuleAttribute{
							{
								Column: "column3",
								Value:  "value3",
							},
						},
					},
				},
			},
		},
	}

	event := newRule.Diff(oldRule)

	// 验证结果
	if len(event.AddTables) != 1 || event.AddTables[0].Name != "table3" {
		t.Errorf("AddTables is not correct")
	}

	if len(event.DeleteTables) != 1 || event.DeleteTables[0].Name != "table2" {
		t.Errorf("DeleteTables is not correct")
	}
	if len(event.UpdateTables) != 0 {
		t.Errorf("UpdateTables is not correct")
	}
}
