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
	"sync"
	"time"
)

import (
	"github.com/pkg/errors"

	"go.uber.org/multierr"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	aranatenant "github.com/arana-db/arana/pkg/runtime/tenant"
	"github.com/arana-db/arana/pkg/runtime/transaction"
	_ "github.com/arana-db/arana/pkg/schema"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/util/log"
)

func Boot(ctx context.Context, provider Discovery) error {
	bt := &Booter{
		discovery: provider,
	}
	return bt.Boot(ctx)
}

type Booter struct {
	discovery Discovery
	watchers  sync.Map
}

func (bt *Booter) Boot(ctx context.Context) error {
	if err := bt.discovery.Init(ctx); err != nil {
		return err
	}

	tenants, err := bt.discovery.ListTenants(ctx)
	if err != nil {
		return errors.Wrap(err, "no tenants found")
	}

	for i := range tenants {
		log.Infof("start boot tenant=%s", tenants[i])
		bt.bootTenant(ctx, tenants[i])
	}

	go func() {
		_ = bt.watchAllTenants(ctx)
	}()

	_ = namespace.List()

	return nil
}

func (bt *Booter) getWatcher(tenant string) *watcher {
	dp := &watcher{
		discovery: bt.discovery,
		tenant:    tenant,
	}
	actual, _ := bt.watchers.LoadOrStore(tenant, dp)
	return actual.(*watcher)
}

func (bt *Booter) bootTenant(ctx context.Context, tenant string) {
	defer func() {
		w := bt.getWatcher(tenant)
		if w.isStarted() {
			return
		}

		go func(w *watcher) {
			defer w.Close()

			log.Infof("[%s] start watching changes", tenant)
			if err := w.watch(ctx); err != nil {
				log.Errorf("[%s] watch changes failed: %v", tenant, err)
			}
		}(w)
	}()

	// FIXME: remove in the future
	_ = bt.discovery.InitTenant(tenant)

	var (
		begin = time.Now()
		errs  []error
	)
	if clusters, err := bt.discovery.ListClusters(ctx, tenant); err != nil {
		errs = append(errs, err)
	} else {
		for _, cluster := range clusters {
			if _, err := bt.discovery.GetCluster(ctx, tenant, cluster); err != nil {
				errs = append(errs, err)
				continue
			}
			ns, err := buildNamespace(ctx, tenant, bt.discovery, cluster)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if err := namespace.Register(tenant, ns); err != nil {
				errs = append(errs, err)
				continue
			}
			security.DefaultTenantManager().PutCluster(tenant, cluster)
			log.Infof("[%s] register namespace %s ok", tenant, cluster)
		}
	}

	if users, err := bt.discovery.ListUsers(ctx, tenant); err != nil {
		errs = append(errs, err)
	} else {
		for i := range users {
			security.DefaultTenantManager().PutUser(tenant, users[i])
			log.Infof("[%s] register user '%s' ok", tenant, users[i].Username)
		}
	}

	if conf, err := bt.discovery.GetSysDB(ctx, tenant); err != nil {
		errs = append(errs, err)
	} else {
		aranatenant.RegisterSysDB(tenant, conf, runtime.NewAtomDB(conf))
	}

	if err := transaction.CreateTrxManager(tenant); err != nil {
		errs = append(errs, err)
	}

	cost := time.Since(begin).Milliseconds()
	if err := multierr.Combine(errs...); err != nil {
		log.Errorf("[%s] boot failed after %dms: %v", tenant, cost, err)
	} else {
		log.Infof("[%s] boot successfully after %dms", tenant, cost)
	}
}

func (bt *Booter) watchAllTenants(ctx context.Context) error {
	chTenants, cancel, err := bt.discovery.WatchTenants(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer cancel()

	for item := range chTenants {
		for i := range item.DeleteTenants {
			tenant := item.DeleteTenants[i]
			clusters := security.DefaultTenantManager().GetClusters(tenant)
			val, ok := bt.watchers.LoadAndDelete(tenant)
			if !ok {
				continue
			}
			wat := val.(*watcher)
			if err = wat.Close(); err == nil {
				log.Infof("[%s] stop watching changes successfully", tenant)
			} else {
				log.Errorf("[%s] stop watching changes failed: %v", tenant, err)
			}

			for _, cluster := range clusters {
				if err = runtime.Unload(tenant, cluster); err == nil {
					log.Infof("[%s] unload runtime '%s' successfully", tenant, cluster)
				} else {
					log.Errorf("[%s] unload runtime '%s' failed: %v", tenant, cluster, err)
				}
			}
		}

		for i := range item.AddTenants {
			bt.bootTenant(ctx, item.AddTenants[i])
		}
	}
	return nil
}

func buildNamespace(ctx context.Context, tenant string, provider Discovery, clusterName string) (*namespace.Namespace, error) {
	var (
		cluster *config.DataSourceCluster
		groups  []string
		err     error
	)

	cluster, err = provider.GetDataSourceCluster(ctx, tenant, clusterName)
	if err != nil {
		return nil, err
	}
	parameters := config.ParametersMap{}
	if cluster != nil {
		parameters = cluster.Parameters
	}

	if groups, err = provider.ListGroups(ctx, tenant, clusterName); err != nil {
		return nil, err
	}

	initCmds := []namespace.Command{
		namespace.UpdateSlowLogger(provider.GetOptions().SlowLogPath, provider.GetOptions().LoggingConfig),
		namespace.UpdateParameters(cluster.Parameters),
		namespace.UpdateSlowThreshold(),
	}

	for _, group := range groups {
		var nodes []string
		if nodes, err = provider.ListNodes(ctx, tenant, clusterName, group); err != nil {
			return nil, err
		}
		for _, it := range nodes {
			var node *config.Node
			if node, err = provider.GetNode(ctx, tenant, clusterName, group, it); err != nil {
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
	if tables, err = provider.ListTables(ctx, tenant, clusterName); err != nil {
		return nil, errors.WithStack(err)
	}

	var ru rule.Rule
	for _, table := range tables {
		var vt *rule.VTable
		if vt, err = provider.GetTable(ctx, tenant, clusterName, table); err != nil {
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
