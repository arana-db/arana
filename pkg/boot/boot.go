// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

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
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/runtime/optimize"
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

func buildNamespace(ctx context.Context, provider Discovery, cluster string) (*namespace.Namespace, error) {
	var (
		groups []string
		err    error
	)

	if groups, err = provider.ListGroups(ctx, cluster); err != nil {
		return nil, err
	}

	var initCmds []namespace.Command
	for _, group := range groups {
		var nodes []string
		if nodes, err = provider.ListNodes(ctx, cluster, group); err != nil {
			return nil, err
		}
		for _, it := range nodes {
			var node *config.Node
			if node, err = provider.GetNode(ctx, cluster, group, it); err != nil {
				return nil, errors.WithStack(err)
			}
			initCmds = append(initCmds, namespace.UpsertDB(group, runtime.NewAtomDB(node)))
		}
	}

	var tables []string
	if tables, err = provider.ListTables(ctx, cluster); err != nil {
		return nil, errors.WithStack(err)
	}

	var ru rule.Rule
	for _, table := range tables {
		var vt *rule.VTable
		if vt, err = provider.GetTable(ctx, cluster, table); err != nil {
			return nil, err
		}
		if vt == nil {
			log.Warnf("no such table %s", table)
			continue
		}
		ru.SetVTable(table, vt)
	}
	initCmds = append(initCmds, namespace.UpdateRule(&ru))

	return namespace.New(cluster, optimize.GetOptimizer(), initCmds...), nil
}
