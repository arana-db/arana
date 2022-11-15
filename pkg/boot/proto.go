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

package boot

import (
	"context"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
)

type Cluster struct {
	Tenant string                `yaml:"tenant" json:"tenant"`
	Name   string                `yaml:"Name" json:"name"`
	Type   config.DataSourceType `yaml:"type" json:"type"`
}

type GroupBody struct {
	Nodes []string `yaml:"nodes" json:"nodes"`
}

type ClusterBody struct {
	Type        config.DataSourceType `yaml:"type" json:"type"`
	SqlMaxLimit int                   `yaml:"sql_max_limit" json:"sql_max_limit,omitempty"`
	Parameters  config.ParametersMap  `yaml:"parameters" json:"parameters,omitempty"`
}

type NodeBody struct {
	Name       string                 `yaml:"name" json:"name"`
	Host       string                 `yaml:"host" json:"host"`
	Port       int                    `yaml:"port" json:"port"`
	Username   string                 `yaml:"username" json:"username"`
	Password   string                 `yaml:"password" json:"password"`
	Database   string                 `yaml:"database" json:"database"`
	Weight     string                 `yaml:"weight" json:"weight"`
	Parameters config.ParametersMap   `yaml:"parameters" json:"parameters,omitempty"`
	ConnProps  map[string]interface{} `yaml:"conn_props" json:"conn_props,omitempty"`
	Labels     map[string]string      `yaml:"labels" json:"labels,omitempty"`
}

type TenantBody struct {
	Users []*config.User `yaml:"users" json:"users"`
}

type TableBody struct {
	Sequence       *config.Sequence  `yaml:"sequence" json:"sequence"`
	AllowFullScan  bool              `yaml:"allow_full_scan" json:"allow_full_scan,omitempty"`
	DbRules        []*config.Rule    `yaml:"db_rules" json:"db_rules"`
	TblRules       []*config.Rule    `yaml:"tbl_rules" json:"tbl_rules"`
	Topology       *config.Topology  `yaml:"topology" json:"topology"`
	ShadowTopology *config.Topology  `yaml:"shadow_topology" json:"shadow_topology"`
	Attributes     map[string]string `yaml:"attributes" json:"attributes"`
}

// ConfigProvider provides configurations.
type ConfigProvider interface {
	// ListTenants list tenants name
	ListTenants(ctx context.Context) ([]string, error)

	// GetTenant returns the tenant info
	GetTenant(ctx context.Context, tenant string) (*config.Tenant, error)

	// ListUsers returns the user list
	ListUsers(ctx context.Context, tenant string) (config.Users, error)

	// ListClusters lists the cluster names.
	ListClusters(ctx context.Context, tenant string) ([]string, error)

	// GetDataSourceCluster returns the dataSourceCluster object
	GetDataSourceCluster(ctx context.Context, tenant, cluster string) (*config.DataSourceCluster, error)

	// GetGroup returns the cluster info
	GetGroup(ctx context.Context, tenant, cluster, group string) (*config.Group, error)

	// GetCluster returns the cluster info
	GetCluster(ctx context.Context, tenant, cluster string) (*Cluster, error)

	// ListGroups lists the group names.
	ListGroups(ctx context.Context, tenant, cluster string) ([]string, error)

	// ListNodes lists the node names.
	ListNodes(ctx context.Context, tenant, cluster, group string) ([]string, error)

	// ListNodesByAdmin lists the node names by admin.
	ListNodesByAdmin(ctx context.Context, tenant string) ([]string, error)

	// GetNode returns the node info.
	GetNode(ctx context.Context, tenant, cluster, group, node string) (*config.Node, error)

	// GetNodeByAdmin returns the node info by admin.
	GetNodeByAdmin(ctx context.Context, tenant, node string) (*config.Node, error)

	// ListTables lists the table names.
	ListTables(ctx context.Context, tenant, cluster string) ([]string, error)

	// GetTable returns the table info.
	GetTable(ctx context.Context, tenant, cluster, table string) (*rule.VTable, error)

	// Import import config into config_center
	Import(ctx context.Context, info *config.Tenant) error
}

// ConfigUpdater represents the mutations of configurations.
// The configuration is designed for structure storage, here is a example in tree-view:
// ── tenants
//
//	├── google
//	│   ├── clusters: [mysql-instance-a,...]
//	│   │   ├── employees
//	│   │   │   ├── groups
//	│   │   │   │   ├── employees_0000
//	│   │   │   │   ├── ...
//	│   │   │   │   └── employees_0007
//	│   │   │   └── tables
//	│   │   │       ├── employee
//	│   │   │       ├── salary
//	│   │   │       └── tax
//	│   │   └── products
//	│   │       └── groups
//	│   │           ├── products_0000
//	│   │           ├── ...
//	│   │           └── products_0007
//	│   └── nodes
//	│       ├── mysql-instance-a
//	│       ├── ...
//	│       └── mysql-instance-x
//	└── apple
//	    ├── ...
//	    └── ...
type ConfigUpdater interface {
	// UpsertTenant upserts a tenant.
	UpsertTenant(ctx context.Context, tenant string, body *TenantBody) error

	// RemoveTenant removes a tenant.
	RemoveTenant(ctx context.Context, tenant string) error

	// UpsertCluster upserts a cluster into an existing tenant.
	UpsertCluster(ctx context.Context, tenant, cluster string, body *ClusterBody) error

	// RemoveCluster removes a cluster from an existing tenant.
	RemoveCluster(ctx context.Context, tenant, cluster string) error

	// UpsertNode upserts a physical node.
	UpsertNode(ctx context.Context, tenant, node string, body *NodeBody) error

	// RemoveNode removes a physical node.
	RemoveNode(ctx context.Context, tenant, node string) error

	// UpsertGroup upserts a group into an existing cluster.
	UpsertGroup(ctx context.Context, tenant, cluster, group string, body *GroupBody) error

	// RemoveGroup removes a group from an existing cluster.
	RemoveGroup(ctx context.Context, tenant, cluster, group string) error

	// BindNode binds a node into an existing cluster group.
	BindNode(ctx context.Context, tenant, cluster, group, node string) error

	// UnbindNode unbinds a node from an existing cluster group.
	UnbindNode(ctx context.Context, tenant, cluster, group, node string) error

	// UpsertTable upserts a new sharding table rule into a cluster.
	UpsertTable(ctx context.Context, tenant, cluster, table string, body *TableBody) error

	// RemoveTable removes a sharding table config from an existing cluster.
	RemoveTable(ctx context.Context, tenant, cluster, table string) error
}

// ConfigWatcher listens for changes in related configuration
type ConfigWatcher interface {
	// WatchTenants watches tenant change
	// return <-chan config.TenantsEvent: listen to this chan to get related event
	// return context.CancelFunc: used to cancel this monitoring, after execution, chan(<-chan config.TenantsEvent) will be closed
	WatchTenants(ctx context.Context) (<-chan config.TenantsEvent, context.CancelFunc, error)
	// WatchNodes watches nodes change
	// return <-chan config.TenantsEvent: listen to this chan to get related event
	// return context.CancelFunc: used to cancel this monitoring, after execution, chan(<-chan config.TenantsEvent) will be closed
	WatchNodes(ctx context.Context, tenant string) (<-chan config.NodesEvent, context.CancelFunc, error)
	// WatchUsers watches users change
	// return <-chan config.TenantsEvent: listen to this chan to get related event
	// return context.CancelFunc: used to cancel this monitoring, after execution, chan(<-chan config.TenantsEvent) will be closed
	WatchUsers(ctx context.Context, tenant string) (<-chan config.UsersEvent, context.CancelFunc, error)
	// WatchClusters watches cluster change
	// return <-chan config.TenantsEvent: listen to this chan to get related event
	// return context.CancelFunc: used to cancel this monitoring, after execution, chan(<-chan config.TenantsEvent) will be closed
	WatchClusters(ctx context.Context, tenant string) (<-chan config.ClustersEvent, context.CancelFunc, error)
	// WatchShardingRule watches sharding rule change
	// return <-chan config.TenantsEvent: listen to this chan to get related event
	// return context.CancelFunc: used to cancel this monitoring, after execution, chan(<-chan config.TenantsEvent) will be closed
	WatchShardingRule(ctx context.Context, tenant string) (<-chan config.ShardingRuleEvent, context.CancelFunc, error)
	// WatchShadowRule watches shadow rule change
	// return <-chan config.TenantsEvent: listen to this chan to get related event
	// return context.CancelFunc: used to cancel this monitoring, after execution, chan(<-chan config.TenantsEvent) will be closed
	WatchShadowRule(ctx context.Context, tenant string) (<-chan config.ShadowRuleEvent, context.CancelFunc, error)
}

type Discovery interface {
	ConfigUpdater
	ConfigProvider
	ConfigWatcher
	// ListListeners lists the listener names
	ListListeners(ctx context.Context) []*config.Listener

	// GetServiceRegistry lists the registry config
	GetServiceRegistry(ctx context.Context) *config.Registry

	// Init initializes discovery with context
	Init(ctx context.Context) error

	// InitTrace distributed tracing
	InitTrace(ctx context.Context) error

	// InitTenant initializes tenant (just a workaround, TBD)
	InitTenant(tenant string) error

	// GetOptions get options
	GetOptions() *BootOptions
}
