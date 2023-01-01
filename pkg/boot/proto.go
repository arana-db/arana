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

	// GetNode returns the node info.
	GetNode(ctx context.Context, tenant, cluster, group, node string) (*config.Node, error)

	// ListTables lists the table names.
	ListTables(ctx context.Context, tenant, cluster string) ([]string, error)

	// GetTable returns the table info.
	GetTable(ctx context.Context, tenant, cluster, table string) (*rule.VTable, error)

	// Import import config into config_center
	Import(ctx context.Context, info *config.Tenant) error
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

	// InitSupervisor initializes supervisor
	InitSupervisor(ctx context.Context) error

	// GetOptions get options
	GetOptions() *config.BootOptions
}
