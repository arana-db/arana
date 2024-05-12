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

package admin

import (
	"context"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

type GroupDTO struct {
	ClusterName string   `json:"clusterName,omitempty"`
	Name        string   `json:"name,omitempty"`
	Nodes       []string `json:"nodes,omitempty"`
}

type TableDTO struct {
	Name           string            `json:"name,omitempty"`
	Sequence       *config.Sequence  `json:"sequence,omitempty"`
	AllowFullScan  bool              `json:"allow_full_scan,omitempty"`
	DbRules        []*config.Rule    `json:"db_rules,omitempty"`
	TblRules       []*config.Rule    `json:"tbl_rules,omitempty"`
	Topology       *config.Topology  `json:"topology,omitempty"`
	ShadowTopology *config.Topology  `json:"shadow_topology,omitempty"`
	Attributes     map[string]string `json:"attributes,omitempty"`
}

type TenantDTO struct {
	Name  string         `json:"name,omitempty"`
	Users []*config.User `json:"users"`
}

type ClusterDTO struct {
	Name        string                `json:"name,omitempty"`
	Type        config.DataSourceType `json:"type"`
	SqlMaxLimit int                   `json:"sql_max_limit,omitempty"`
	Parameters  config.ParametersMap  `json:"parameters,omitempty"`
	Groups      []string              `json:"groups"`
}

type NodeDTO struct {
	Name       string                 `json:"name,omitempty"`
	Host       string                 `json:"host"`
	Port       int                    `json:"port"`
	Username   string                 `json:"username"`
	Password   string                 `json:"password"`
	Database   string                 `json:"database"`
	Weight     string                 `json:"weight"`
	Parameters config.ParametersMap   `json:"parameters,omitempty"`
	ConnProps  map[string]interface{} `json:"conn_props,omitempty"`
	Labels     map[string]string      `json:"labels,omitempty"`
}

type ServiceInstanceDTO struct {
	// ID is the unique instance ID as registered.
	ID string `json:"id"`
	// Name is the service name as registered.
	Name string `json:"name"`
	// Version is the version of the compiled.
	Version string `json:"version"`
	// Endpoint addresses of the service instance.
	Endpoint *config.Listener
}

// XConfigWriter represents the mutations of configurations.
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
type configWriter interface {
	// UpsertTenant upserts a tenant.
	UpsertTenant(ctx context.Context, tenant string, body *TenantDTO) error

	// RemoveTenant removes a tenant.
	RemoveTenant(ctx context.Context, tenant string) error

	// UpsertCluster upserts a cluster into an existing tenant.
	UpsertCluster(ctx context.Context, tenant, cluster string, body *ClusterDTO) error

	// ExtendCluster extends a cluster in an existing tenant.
	ExtendCluster(ctx context.Context, tenant, cluster string, body *ClusterDTO) error

	// RemoveCluster removes a cluster from an existing tenant.
	RemoveCluster(ctx context.Context, tenant, cluster string) error

	// UpsertNode upserts a physical node.
	UpsertNode(ctx context.Context, tenant, node string, body *NodeDTO) error

	// RemoveNode removes a physical node.
	RemoveNode(ctx context.Context, tenant, node string) error

	// UpsertGroup upserts a group into an existing cluster.
	UpsertGroup(ctx context.Context, tenant, cluster, group string, body *GroupDTO) error

	// RemoveGroup removes a group from an existing cluster.
	RemoveGroup(ctx context.Context, tenant, cluster, group string) error

	// BindNode binds a node into an existing cluster group.
	BindNode(ctx context.Context, tenant, cluster, group, node string) error

	// UnbindNode unbinds a node from an existing cluster group.
	UnbindNode(ctx context.Context, tenant, cluster, group, node string) error

	// UpsertTable upserts a new sharding table rule into a cluster.
	UpsertTable(ctx context.Context, tenant, cluster, table string, body *TableDTO) error

	// RemoveTable removes a sharding table config from an existing cluster.
	RemoveTable(ctx context.Context, tenant, cluster, table string) error

	// UpsertUser upserts a user.
	UpsertUser(ctx context.Context, tenant string, user *config.User, username string) error

	// RemoveUser removes a user.
	RemoveUser(ctx context.Context, tenant string, username string) error
}

type configReader interface {
	// ListTenants lists all tenants.
	ListTenants(ctx context.Context) ([]*TenantDTO, error)

	// ListNodes lists all nodes of tenant.
	ListNodes(ctx context.Context, tenant string) ([]*NodeDTO, error)

	// ListClusters lists all clusters of tenant.
	ListClusters(ctx context.Context, tenant string) ([]*ClusterDTO, error)

	// ListDBGroups lists all db groups of cluster.
	ListDBGroups(ctx context.Context, tenant, cluster string) ([]*GroupDTO, error)

	// ListTables lists all tables of cluster.
	ListTables(ctx context.Context, tenant, cluster string) ([]*TableDTO, error)
}

// ConfigService represents a service to read/write configurations.
type ConfigService interface {
	configWriter
	configReader
}
