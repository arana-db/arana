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

type EventType int32

const (
	_ EventType = iota
	EventTypeTenants
	EventTypeUsers
	EventTypeNodes
	EventTypeClusters
	EventTypeShardingRule
	EventTypeShadowRule
)

type (
	Event interface {
		Type() EventType
	}

	// TenantsEvent tenants event
	TenantsEvent struct {
		AddTenants    Tenants
		DeleteTenants Tenants
	}

	// UsersEvent users event
	UsersEvent struct {
		AddUsers    Users
		UpdateUsers Users
		DeleteUsers Users
	}

	// ClustersEvent clusters event
	ClustersEvent struct {
		AddClusters    Clusters
		DeleteClusters Clusters
		UpdateClusters []*ClusterEvent
	}

	// ClusterEvent cluster event
	ClusterEvent struct {
		Name        string
		Type        DataSourceType
		SqlMaxLimit int
		Parameters  ParametersMap
		GroupsEvent *GroupsEvent
	}

	// GroupsEvent groups event
	GroupsEvent struct {
		AddGroups    Groups
		UpdateGroups Groups
		DeleteGroups Groups
	}

	// GroupEvent group event
	GroupEvent struct {
		Name        string
		AddNodes    Nodes
		UpdateNodes Nodes
		DeleteNodes Nodes
	}

	// ShardingRuleEvent sharding rule event
	ShardingRuleEvent struct {
		AddTables    []*Table
		UpdateTables []*Table
		DeleteTables []*Table
	}

	// ShadowRuleEvent shadow rule event
	ShadowRuleEvent struct {
		AddTables    []*ShadowTable
		UpdateTables []*ShadowTable
		DeleteTables []*ShadowTable
	}

	// NodesEvent nodes event
	NodesEvent struct {
		AddNodes    []*Node
		UpdateNodes []*Node
		DeleteNodes []*Node
	}
)

func (e TenantsEvent) Type() EventType {
	return EventTypeTenants
}

func (e NodesEvent) Type() EventType {
	return EventTypeNodes
}

func (e UsersEvent) Type() EventType {
	return EventTypeUsers
}

func (e ClustersEvent) Type() EventType {
	return EventTypeClusters
}

func (e ShardingRuleEvent) Type() EventType {
	return EventTypeShardingRule
}

func (e ShadowRuleEvent) Type() EventType {
	return EventTypeShadowRule
}
