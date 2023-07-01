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

	uatomic "go.uber.org/atomic"

	"golang.org/x/exp/slices"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/util/log"
)

type watcher struct {
	started   uatomic.Bool
	discovery Discovery
	tenant    string
	cancels   []context.CancelFunc
	cancel    context.CancelFunc
}

func (d *watcher) Close() error {
	if !d.isStarted() {
		return nil
	}
	for _, next := range d.cancels {
		next()
	}
	if d.cancel != nil {
		d.cancel()
	}
	return nil
}

func (d *watcher) isStarted() bool {
	return d.started.Load()
}

func (d *watcher) watch(ctx context.Context) error {
	if !d.started.CAS(false, true) {
		return nil
	}

	var (
		chRules       <-chan config.ShardingRuleEvent
		chShadowRules <-chan config.ShadowRuleEvent
		chNodes       <-chan config.NodesEvent
		chUsers       <-chan config.UsersEvent
		chClusters    <-chan config.ClustersEvent
		cancel        context.CancelFunc
		err           error
	)

	ctx, d.cancel = context.WithCancel(ctx)

	// rules
	if chRules, cancel, err = d.discovery.WatchShardingRule(ctx, d.tenant); err != nil {
		return errors.Wrap(err, "failed to watch sharding-rule")
	}
	d.cancels = append(d.cancels, cancel)

	// shadow-rules
	if chShadowRules, cancel, err = d.discovery.WatchShadowRule(ctx, d.tenant); err != nil {
		return errors.Wrap(err, "failed to watch shadow-rule")
	}
	d.cancels = append(d.cancels, cancel)

	// users
	if chUsers, cancel, err = d.discovery.WatchUsers(ctx, d.tenant); err != nil {
		return errors.Wrap(err, "failed to watch users")
	}
	d.cancels = append(d.cancels, cancel)

	// nodes
	if chNodes, cancel, err = d.discovery.WatchNodes(ctx, d.tenant); err != nil {
		return errors.Wrap(err, "failed to watch nodes")
	}
	d.cancels = append(d.cancels, cancel)

	// clusters
	if chClusters, cancel, err = d.discovery.WatchClusters(ctx, d.tenant); err != nil {
		return errors.Wrap(err, "failed to watch cluster")
	}
	d.cancels = append(d.cancels, cancel)

L:
	for {
		select {
		case <-ctx.Done():
			break L
		case item := <-chNodes:
			for i := range item.DeleteNodes {
				if err := d.onNodeDel(ctx, item.DeleteNodes[i].Name); err != nil {
					log.Errorf("[%s] handle event NODE-DEL failed: %v", d.tenant, err)
				}
			}
			for i := range item.AddNodes {
				if err := d.onNodeAdd(ctx, item.AddNodes[i]); err != nil {
					log.Errorf("[%s] handle event NODE-ADD failed: %v", d.tenant, err)
				}
			}
			for i := range item.UpdateNodes {
				if err := d.onNodeUpdate(ctx, item.UpdateNodes[i]); err != nil {
					log.Errorf("[%s] handle event NODE-CHG failed: %v", d.tenant, err)
				}
			}

		case item := <-chUsers:
			for i := range item.DeleteUsers {
				if err := d.onUserDel(ctx, item.DeleteUsers[i].Username); err != nil {
					log.Errorf("[%s] handle event USER-DEL failed: %v", d.tenant, err)
				}
			}
			for i := range item.AddUsers {
				if err := d.onUserAdd(ctx, item.AddUsers[i]); err != nil {
					log.Errorf("[%s] handle event USER-DEL failed: %v", d.tenant, err)
				}
			}
			for i := range item.UpdateUsers {
				if err := d.onUserUpdate(ctx, item.UpdateUsers[i]); err != nil {
					log.Errorf("[%s] handle event USER-DEL failed: %v", d.tenant, err)
				}
			}
		case item := <-chRules:
			for i := range item.DeleteTables {
				if err := d.onTableDel(ctx, item.DeleteTables[i].Name); err != nil {
					log.Errorf("[%s] handle event TABLE-DEL failed: %v", err)
				}
			}

			for i := range item.AddTables {
				if err := d.onTableAdd(ctx, item.AddTables[i]); err != nil {
					log.Errorf("[%s] handle event TABLE-ADD failed: %v", err)
				}
			}
			for i := range item.UpdateTables {
				if err := d.onTableChange(ctx, item.UpdateTables[i]); err != nil {
					log.Errorf("[%s] handle event TABLE-CHG failed: %v", d.tenant, err)
				}
			}
		case item := <-chShadowRules:
			// TODO: need implementation
			_ = item
			// panic("implement me")
		case item := <-chClusters:
			for i := range item.DeleteClusters {
				if err := d.onClusterDel(ctx, item.DeleteClusters[i].Name); err != nil {
					log.Errorf("[%s] handle CLUSTER-DEL failed: %v", d.tenant, err)
				}
			}
			for i := range item.AddClusters {
				if err := d.onClusterAdd(ctx, item.AddClusters[i]); err != nil {
					log.Errorf("[%s] handle CLUSTER-ADD failed: %v", d.tenant, err)
				}
			}

			for i := range item.UpdateClusters {
				next := item.UpdateClusters[i]

				// TODO: 1. handle basic parameters modification

				if next.GroupsEvent == nil {
					continue
				}

				// 2. handle group modification
				for j := range next.GroupsEvent.DeleteGroups {
					if err := d.onGroupDel(ctx, next.Name, next.GroupsEvent.DeleteGroups[j].Name); err != nil {
						log.Errorf("[%s] handle GROUP-DEL failed: %v", d.tenant, err)
					}
				}
				for j := range next.GroupsEvent.AddGroups {
					if err := d.onGroupAdd(ctx, next.Name, next.GroupsEvent.AddGroups[j]); err != nil {
						log.Errorf("[%s] handle GROUP-ADD failed: %v", d.tenant, err)
					}
				}
				for j := range next.GroupsEvent.UpdateGroups {
					if err := d.onGroupUpdate(ctx, next.Name, next.GroupsEvent.UpdateGroups[j]); err != nil {
						log.Errorf("[%s] handle GROUP-UPDATE failed: %v", d.tenant, err)
					}
				}
			}
		}
	}

	return nil
}

func (d *watcher) onGroupUpdate(ctx context.Context, cluster string, group *config.Group) error {
	rt, err := d.getRuntime(cluster)
	if err != nil {
		return errors.WithStack(err)
	}

	prev := make(map[string]struct{})
	for _, it := range rt.Namespace().DBs(group.Name) {
		prev[it.ID()] = struct{}{}
	}
	next := make(map[string]struct{})
	for i := range group.Nodes {
		next[group.Nodes[i]] = struct{}{}
	}

	var (
		removes []string
		adds    []string
	)
	// diff && generate remove list
	for k := range prev {
		if _, ok := next[k]; !ok {
			removes = append(removes, k)
		}
	}
	// diff && generate add list
	for k := range next {
		if _, ok := prev[k]; !ok {
			adds = append(adds, k)
		}
	}

	for i := range removes {
		if err := rt.Namespace().EnqueueCommand(namespace.RemoveNode(group.Name, removes[i])); err != nil {
			log.Errorf("failed to enqueue remove node command: %v", err)
		}
	}

	var params config.ParametersMap
	for i := range adds {
		n, _ := d.discovery.GetNode(context.Background(), d.tenant, cluster, group.Name, adds[i])
		if n == nil {
			log.Warnf("failed to add node: no such node '%s'", adds[i])
			continue
		}

		// merge group level params
		newParams := make(config.ParametersMap)
		for k, v := range n.Parameters {
			k, v := k, v
			newParams[k] = v
		}
		n.Parameters = newParams
		if params == nil {
			if clu, _ := d.discovery.GetDataSourceCluster(ctx, d.tenant, cluster); clu != nil {
				params = clu.Parameters
			}
		}
		n.Parameters.Merge(params)

		if err := rt.Namespace().EnqueueCommand(namespace.UpsertDB(group.Name, runtime.NewAtomDB(n))); err != nil {
			log.Errorf("failed to enqueue add node command: %v", err)
		}
	}

	return nil
}

func (d *watcher) getRuntime(cluster string) (runtime.Runtime, error) {
	return runtime.Load(d.tenant, cluster)
}

func (d *watcher) onGroupDel(_ context.Context, cluster, group string) error {
	rt, err := d.getRuntime(cluster)
	if err != nil {
		return errors.Wrapf(err, "cannot load runtime: tenant=%s, schema=%s", d.tenant, cluster)
	}

	if err = rt.Namespace().EnqueueCommand(namespace.RemoveGroup(group)); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (d *watcher) onGroupAdd(ctx context.Context, cluster string, group *config.Group) error {
	rt, err := d.getRuntime(cluster)
	if err != nil {
		return errors.WithStack(err)
	}
	if rt == nil {
		return nil
	}

	for i := range group.Nodes {
		nodeId := group.Nodes[i]
		node, err := d.discovery.GetNode(ctx, d.tenant, cluster, group.Name, nodeId)
		if err != nil {
			log.Errorf("[%s] cannot get node '%s': %v", d.tenant, nodeId, err)
			continue
		}
		if node == nil {
			log.Errorf("[%s] no node '%s' found", d.tenant, nodeId)
			continue
		}

		if err := rt.Namespace().EnqueueCommand(namespace.UpsertDB(group.Name, runtime.NewAtomDB(node))); err != nil {
			log.Errorf("[%s] failed to enqueue upsert-db command: %v", err)
		}
	}

	return nil
}

func (d *watcher) onClusterAdd(ctx context.Context, cluster *config.DataSourceCluster) error {
	clusterParams := make(config.ParametersMap)
	if cluster.Parameters != nil {
		for k, v := range cluster.Parameters {
			k, v := k, v
			clusterParams[k] = v
		}
	}

	var cmds []namespace.Command
	for _, group := range cluster.Groups {
		for _, nodeId := range group.Nodes {
			node, err := d.discovery.GetNode(ctx, d.tenant, cluster.Name, group.Name, nodeId)
			if err != nil {
				return errors.WithStack(err)
			}

			clonedNode := *node
			nodeParams := make(config.ParametersMap)
			nodeParams.Merge(node.Parameters)
			nodeParams.Merge(clusterParams)
			clonedNode.Parameters = nodeParams
			cmds = append(cmds, namespace.UpsertDB(group.Name, runtime.NewAtomDB(&clonedNode)))
		}
	}

	tables, err := d.discovery.ListTables(ctx, d.tenant, cluster.Name)
	if err != nil {
		return errors.WithStack(err)
	}

	var ru rule.Rule
	for _, table := range tables {
		var vt *rule.VTable
		if vt, err = d.discovery.GetTable(ctx, d.tenant, cluster.Name, table); err != nil {
			return errors.WithStack(err)
		}
		if vt == nil {
			log.Warnf("no such table %s", table)
			continue
		}
		ru.SetVTable(table, vt)
	}
	cmds = append(cmds, namespace.UpdateRule(&ru))
	ns, err := namespace.New(cluster.Name, cmds...)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := namespace.Register(d.tenant, ns); err != nil {
		return errors.WithStack(err)
	}
	security.DefaultTenantManager().PutCluster(d.tenant, cluster.Name)

	return nil
}

func (d *watcher) onClusterDel(_ context.Context, clusterName string) error {
	if err := runtime.Unload(d.tenant, clusterName); err != nil {
		return errors.WithStack(err)
	}

	log.Infof("[%s] CLUSTER-DEL: unload cluster '%s' successfully", d.tenant, clusterName)

	return nil
}

func (d *watcher) onUserUpdate(_ context.Context, user *config.User) error {
	security.DefaultTenantManager().PutUser(d.tenant, user)
	log.Infof("[%s] USER-CHG: update user '%s' successfully", d.tenant, user.Username)
	return nil
}

func (d *watcher) onUserAdd(_ context.Context, user *config.User) error {
	security.DefaultTenantManager().PutUser(d.tenant, user)
	log.Infof("[%s] USER-ADD: add user '%s' successfully", d.tenant, user.Username)
	return nil
}

func (d *watcher) onUserDel(_ context.Context, username string) error {
	security.DefaultTenantManager().RemoveUser(d.tenant, username)
	log.Infof("[%s] USER-DEL: remove user '%s' successfully", d.tenant, username)
	return nil
}

func (d *watcher) onNodeDel(_ context.Context, node string) error {
	clusters := security.DefaultTenantManager().GetClusters(d.tenant)
	for _, cluster := range clusters {
		rt, err := runtime.Load(d.tenant, cluster)
		if err != nil {
			continue
		}
		groups := rt.Namespace().DBGroups()
		for i := range groups {
			group := groups[i]
			for _, db := range rt.Namespace().DBs(group) {
				if db.ID() != node {
					continue
				}
				if err := rt.Namespace().EnqueueCommand(namespace.RemoveNode(group, node)); err != nil {
					log.Errorf("failed to enqueue delete-node command: %v", err)
				}
			}
		}
	}

	return nil
}

func (d *watcher) onNodeAdd(ctx context.Context, node *config.Node) error {
	// FIXME: do nothing???
	_ = ctx
	_ = node
	return nil
}

func (d *watcher) onNodeUpdate(ctx context.Context, node *config.Node) error {
	clusters := security.DefaultTenantManager().GetClusters(d.tenant)

	var paramsMap map[string]config.ParametersMap

	updateNode := func(rt runtime.Runtime, cluster, group string) {
		clonedNode := *node

		newParams := make(config.ParametersMap)
		for k, v := range clonedNode.Parameters {
			k, v := k, v
			newParams[k] = v
		}
		clonedNode.Parameters = newParams

		// merge group level params
		params, ok := paramsMap[cluster]
		if !ok {
			if clu, _ := d.discovery.GetDataSourceCluster(ctx, d.tenant, cluster); clu != nil {
				params = clu.Parameters
				paramsMap[cluster] = params
			}
		}
		clonedNode.Parameters.Merge(params)

		if err := rt.Namespace().EnqueueCommand(namespace.UpsertDB(group, runtime.NewAtomDB(&clonedNode))); err != nil {
			log.Errorf("failed to enqueue update node: %v", err)
		}
	}

	for _, cluster := range clusters {
		rt, err := runtime.Load(d.tenant, cluster)
		if err != nil {
			log.Errorf("cannot find runtime: tenant=%s, cluster=%s", d.tenant, cluster)
			continue
		}

		groups := rt.Namespace().DBGroups()
		for _, group := range groups {
			for _, db := range rt.Namespace().DBs(group) {
				if db.ID() != node.Name {
					continue
				}
				updateNode(rt, cluster, group)
			}
		}
	}

	return nil
}

func (d *watcher) onTableDel(_ context.Context, name string) error {
	db, tbl, err := parseDatabaseAndTable(name)
	if err != nil {
		return errors.WithStack(err)
	}

	clusters := security.DefaultTenantManager().GetClusters(d.tenant)
	if !slices.Contains(clusters, db) {
		log.Warnf("[%s] ignore TABLE-DEL: no such namespace '%s'", d.tenant, db)
		return nil
	}

	ns := namespace.Load(d.tenant, db)
	if ns == nil {
		log.Warnf("[%s] ignore TABLE-DEL: no such namespace '%s'", d.tenant, db)
		return nil
	}

	if !ns.Rule().RemoveVTable(tbl) {
		log.Warnf("[%s] ignore TABLE-DEL: no such table '%s.%s'", d.tenant, db, tbl)
		return nil
	}

	log.Infof("[%s] TABLE-DEL: remove virtual table '%s.%s' successfully", d.tenant, db, tbl)

	return nil
}

func (d *watcher) onTableAdd(_ context.Context, table *config.Table) error {
	db, tbl, err := parseDatabaseAndTable(table.Name)
	if err != nil {
		return errors.WithStack(err)
	}
	clusters := security.DefaultTenantManager().GetClusters(d.tenant)
	if !slices.Contains(clusters, db) {
		log.Warnf("[%s] TABLE-ADD: no such cluster '%s'", d.tenant, db)
		return nil
	}
	ns := namespace.Load(d.tenant, db)
	if ns == nil {
		log.Warnf("[%s] TABLE-ADD: no such namespace '%s'", d.tenant, db)
		return nil
	}

	vtab, err := makeVTable(tbl, table)
	if err != nil {
		return errors.WithStack(err)
	}

	ns.Rule().SetVTable(tbl, vtab)
	return nil
}

func (d *watcher) onTableChange(_ context.Context, table *config.Table) error {
	db, tbl, err := parseDatabaseAndTable(table.Name)
	if err != nil {
		return errors.WithStack(err)
	}
	clusters := security.DefaultTenantManager().GetClusters(d.tenant)
	if !slices.Contains(clusters, db) {
		return nil
	}
	ns := namespace.Load(d.tenant, db)
	if ns == nil {
		log.Warnf("[%s] TABLE-CHG: no such namespace '%s'", d.tenant, db)
		return nil
	}

	vtab, err := makeVTable(tbl, table)
	if err != nil {
		return errors.WithStack(err)
	}

	ns.Rule().SetVTable(tbl, vtab)

	return nil
}
