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

//ConfigChangeEvent
type ConfigChangeEvent struct {
	TenantsEvent      ChangeTenantsEvent
	ClustersEvent     ChangeClustersEvent
	ShardingRuleEvent ShardingRuleEvent
}

//ChangeTenantsEvent
type ChangeTenantsEvent struct {
	AddTenants    []TenantEvent
	UpdateTenants []TenantEvent
	DeleteTenants []TenantEvent
}

//TenantEvent
type TenantEvent struct {
	Name        string
	AddUsers    Users
	UpdateUsers Users
	DeleteUsers Users
}

//ChangeClustersEvent
type ChangeClustersEvent struct {
	AddCluster    []ClusterEvent
	UpdateCluster []ClusterEvent
	DeleteCluster []ClusterEvent
}

//ClusterEvent
type ClusterEvent struct {
	Name         string
	Type         DataSourceType
	SqlMaxLimit  int
	Tenant       string
	AddGroups    []GroupEvent
	UpdateGroups []GroupEvent
	DeleteGroups []GroupEvent
}

//GroupEvent
type GroupEvent struct {
	Name        string
	AddNodes    Nodes
	UpdateNodes Nodes
	DeleteNodes Nodes
}

//ShardingRuleEvent
type ShardingRuleEvent struct {
	AddTables    []*Table
	UpdateTables []*Table
	DeleteTables []*Table
}
