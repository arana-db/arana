/*
 *  Licensed to Apache Software Foundation (ASF) under one or more contributor
 *  license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright
 *  ownership. Apache Software Foundation (ASF) licenses this file to you under
 *  the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package config

import (
	"fmt"
	"path/filepath"
)

type PathInfo struct {
	DefaultConfigSpecPath               PathKey
	DefaultTenantBaseConfigPath         PathKey
	DefaultConfigDataNodesPath          PathKey
	DefaultConfigDataUsersPath          PathKey
	DefaultConfigDataSourceClustersPath PathKey
	DefaultConfigDataShardingRulePath   PathKey
	DefaultConfigDataShadowRulePath     PathKey

	ConfigKeyMapping   map[PathKey]string
	ConfigEventMapping map[PathKey]EventType
	BuildEventMapping  map[EventType]func(pre, cur *Tenant) Event
	ConfigValSupplier  map[PathKey]func(cfg *Tenant) interface{}
}

func NewPathInfo(tenant string) *PathInfo {

	p := &PathInfo{}

	p.DefaultTenantBaseConfigPath = PathKey(filepath.Join(string(DefaultRootPath), fmt.Sprintf("tenants/%s", tenant)))
	p.DefaultConfigSpecPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "spec"))
	p.DefaultConfigDataNodesPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "nodes"))
	p.DefaultConfigDataUsersPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "users"))
	p.DefaultConfigDataSourceClustersPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "dataSourceClusters"))
	p.DefaultConfigDataShardingRulePath = PathKey(filepath.Join(string(p.DefaultConfigDataSourceClustersPath), "shardingRule"))
	p.DefaultConfigDataShadowRulePath = PathKey(filepath.Join(string(p.DefaultConfigDataSourceClustersPath), "shadowRule"))

	p.ConfigEventMapping = map[PathKey]EventType{
		p.DefaultConfigDataUsersPath:          EventTypeUsers,
		p.DefaultConfigDataNodesPath:          EventTypeNodes,
		p.DefaultConfigDataSourceClustersPath: EventTypeClusters,
		p.DefaultConfigDataShardingRulePath:   EventTypeShardingRule,
		p.DefaultConfigDataShadowRulePath:     EventTypeShadowRule,
	}

	p.ConfigValSupplier = map[PathKey]func(cfg *Tenant) interface{}{
		p.DefaultConfigSpecPath: func(cfg *Tenant) interface{} {
			return &cfg.Spec
		},
		p.DefaultConfigDataUsersPath: func(cfg *Tenant) interface{} {
			return &cfg.Users
		},
		p.DefaultConfigDataSourceClustersPath: func(cfg *Tenant) interface{} {
			return &cfg.DataSourceClusters
		},
		p.DefaultConfigDataNodesPath: func(cfg *Tenant) interface{} {
			return &cfg.Nodes
		},
		p.DefaultConfigDataShardingRulePath: func(cfg *Tenant) interface{} {
			return cfg.ShardingRule
		},
		p.DefaultConfigDataShadowRulePath: func(cfg *Tenant) interface{} {
			return cfg.ShadowRule
		},
	}

	p.ConfigKeyMapping = map[PathKey]string{
		p.DefaultConfigSpecPath:               ConfigItemSpec,
		p.DefaultConfigDataUsersPath:          ConfigItemUsers,
		p.DefaultConfigDataSourceClustersPath: ConfigItemClusters,
		p.DefaultConfigDataShardingRulePath:   ConfigItemShardingRule,
		p.DefaultConfigDataNodesPath:          ConfigItemNodes,
		p.DefaultConfigDataShadowRulePath:     ConfigItemShadowRule,
	}

	p.BuildEventMapping = map[EventType]func(pre *Tenant, cur *Tenant) Event{
		EventTypeNodes: func(pre, cur *Tenant) Event {
			return Nodes(cur.Nodes).Diff(pre.Nodes)
		},
		EventTypeUsers: func(pre, cur *Tenant) Event {
			return Users(cur.Users).Diff(pre.Users)
		},
		EventTypeClusters: func(pre, cur *Tenant) Event {
			return Clusters(cur.DataSourceClusters).Diff(pre.DataSourceClusters)
		},
		EventTypeShardingRule: func(pre, cur *Tenant) Event {
			return cur.ShardingRule.Diff(pre.ShardingRule)
		},
		EventTypeShadowRule: func(pre, cur *Tenant) Event {
			return cur.ShadowRule.Diff(pre.ShadowRule)
		},
	}

	return p
}
