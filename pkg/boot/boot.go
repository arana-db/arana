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
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	_ "github.com/arana-db/arana/pkg/schema"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/util/log"
)

func Boot(ctx context.Context, provider Discovery) error {
	if err := provider.Init(ctx); err != nil {
		return err
	}

	clusters, err := provider.ListClusters(ctx)
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		var (
			c  *Cluster
			ns *namespace.Namespace
		)

		if c, err = provider.GetCluster(ctx, cluster); err != nil {
			continue
		}

		ctx = rcontext.WithTenant(ctx, c.Tenant)

		if ns, err = buildNamespace(ctx, provider, cluster); err != nil {
			log.Errorf("build namespace %s failed: %v", cluster, err)
			continue
		}
		if err = namespace.Register(ns); err != nil {
			log.Errorf("register namespace %s failed: %v", cluster, err)
			continue
		}
		log.Infof("register namespace %s successfully", cluster)
		security.DefaultTenantManager().PutCluster(c.Tenant, cluster)
	}

	var tenants []string
	if tenants, err = provider.ListTenants(ctx); err != nil {
		return errors.Wrap(err, "no tenants found")
	}

	for _, tenant := range tenants {
		var t *config.Tenant
		if t, err = provider.GetTenant(ctx, tenant); err != nil {
			log.Errorf("failed to get tenant %s: %v", tenant, err)
			continue
		}
		for _, it := range t.Users {
			security.DefaultTenantManager().PutUser(tenant, it)
		}
	}

	return nil
}

func buildNamespace(ctx context.Context, provider Discovery, clusterName string) (*namespace.Namespace, error) {
	var (
		cluster *config.DataSourceCluster
		groups  []string
		err     error
	)

	cluster, err = provider.GetDataSourceCluster(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	parameters := config.ParametersMap{}
	if cluster != nil {
		parameters = cluster.Parameters
	}

	if groups, err = provider.ListGroups(ctx, clusterName); err != nil {
		return nil, err
	}

	var initCmds []namespace.Command
	for _, group := range groups {
		var nodes []string
		if nodes, err = provider.ListNodes(ctx, clusterName, group); err != nil {
			return nil, err
		}
		for _, it := range nodes {
			var node *config.Node
			if node, err = provider.GetNode(ctx, clusterName, group, it); err != nil {
				return nil, errors.WithStack(err)
			}
			if node.Parameters == nil {
				node.Parameters = config.ParametersMap{}
			}
			node.Parameters.Merge(parameters)
			initCmds = append(initCmds, namespace.UpsertDB(group, runtime.NewAtomDB(node)))
		}
	}

	var tables []string
	if tables, err = provider.ListTables(ctx, clusterName); err != nil {
		return nil, errors.WithStack(err)
	}

	var ru rule.Rule
	for _, table := range tables {
		var vt *rule.VTable
		if vt, err = provider.GetTable(ctx, clusterName, table); err != nil {
			return nil, err
		}
		if vt == nil {
			log.Warnf("no such table %s", table)
			continue
		}
		ru.SetVTable(table, vt)
	}
	initCmds = append(initCmds, namespace.UpdateRule(&ru))

	return namespace.New(clusterName, initCmds...)
}
