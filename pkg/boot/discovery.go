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
	Tenant string
	Type   config.DataSourceType
}

type Discovery interface {
	Init(ctx context.Context) error
	ListTenants(ctx context.Context) ([]string, error)
	ListListeners(ctx context.Context) ([]*config.Listener, error)
	ListFilters(ctx context.Context) ([]*config.Filter, error)
	// ListClusters lists the cluster names.
	ListClusters(ctx context.Context) ([]string, error)
	// ListGroups lists the group names.
	ListGroups(ctx context.Context, cluster string) ([]string, error)
	// ListNodes lists the node names.
	ListNodes(ctx context.Context, cluster, group string) ([]string, error)
	// ListTables lists the table names.
	ListTables(ctx context.Context, cluster string) ([]string, error)
	// GetNode returns the node info.
	GetNode(ctx context.Context, cluster, group, node string) (*config.Node, error)
	// GetTable returns the table info.
	GetTable(ctx context.Context, cluster, table string) (*rule.VTable, error)
	GetTenant(ctx context.Context, tenant string) (*config.Tenant, error)
	GetCluster(ctx context.Context, cluster string) (*Cluster, error)
}
