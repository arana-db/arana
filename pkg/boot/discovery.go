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
	"github.com/creasty/defaults"

	"github.com/pkg/errors"

	uatomic "go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/trace"
	uconfig "github.com/arana-db/arana/pkg/util/config"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/pkg/util/misc"
)

var _ Discovery = (*discovery)(nil)

var (
	ErrorNoTenant            = errors.New("no tenant")
	ErrorNoDataSourceCluster = errors.New("no datasourceCluster")
	ErrorNoGroup             = errors.New("no group")
)

type discovery struct {
	inited  uatomic.Bool
	path    string
	options *config.BootOptions

	tenantOp config.TenantOperator
	centers  map[string]config.Center
}

func (fp *discovery) Import(ctx context.Context, info *config.Tenant) error {
	op, ok := fp.centers[info.Name]
	if !ok {
		return ErrorNoTenant
	}

	return op.Import(ctx, info)
}

func (fp *discovery) Init(ctx context.Context) error {
	if !fp.inited.CAS(false, true) {
		return nil
	}

	cfg, err := config.LoadBootOptions(fp.path)
	if err != nil {
		return err
	}
	uconfig.IsEnableLocalMathCompu(cfg.Spec.EnableLocalMathComputation)
	fp.options = cfg

	if err = config.Init(*fp.options.Config, fp.options.Spec.APIVersion); err != nil {
		return err
	}

	fp.tenantOp, err = config.NewTenantOperator(config.GetStoreOperate())
	if err != nil {
		return err
	}
	return fp.initAllConfigCenter()
}

func (fp *discovery) InitTenant(tenant string) error {
	options := *fp.options.Config
	if len(options.Options) == 0 {
		options.Options = map[string]interface{}{}
	}
	options.Options["tenant"] = tenant

	var err error

	fp.centers[tenant], err = config.NewCenter(tenant, config.GetStoreOperate(),
		config.WithCacheable(true),
		config.WithReader(true),
		config.WithWatcher(true),
		config.WithWriter(true),
	)

	return err
}

func (fp *discovery) initAllConfigCenter() error {
	tenants := fp.tenantOp.ListTenants()
	for i := range tenants {
		if err := fp.InitTenant(tenants[i]); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (fp *discovery) GetDataSourceCluster(ctx context.Context, tenant, cluster string) (*config.DataSourceCluster, error) {
	return fp.loadCluster(tenant, cluster)
}

func (fp *discovery) GetGroup(ctx context.Context, tenant, cluster, group string) (*config.Group, error) {
	exist, ok := fp.loadGroup(tenant, cluster, group)
	if !ok {
		return nil, nil
	}

	return exist, nil
}

func (fp *discovery) GetCluster(ctx context.Context, tenant, cluster string) (*Cluster, error) {
	exist, err := fp.loadCluster(tenant, cluster)
	if err != nil {
		return nil, err
	}

	return &Cluster{
		Name:   exist.Name,
		Tenant: tenant,
		Type:   exist.Type,
	}, nil
}

func (fp *discovery) ListTenants(ctx context.Context) ([]string, error) {
	return fp.tenantOp.ListTenants(), nil
}

func (fp *discovery) GetTenant(ctx context.Context, tenant string) (*config.Tenant, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func (fp *discovery) ListUsers(ctx context.Context, tenant string) (config.Users, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	if cfg == nil {
		return nil, nil
	}

	return cfg.Users, nil
}

func (fp *discovery) InitTrace(ctx context.Context) error {
	if fp.options.Trace == nil {
		fp.options.Trace = &config.Trace{}
	}
	if err := defaults.Set(fp.options.Trace); err != nil {
		return err
	}
	return trace.Initialize(ctx, fp.options.Trace)
}

func (fp *discovery) InitSupervisor(ctx context.Context) error {
	if fp.options.Supervisor != nil {
		security.DefaultTenantManager().SetSupervisor(fp.options.Supervisor)
	}
	return nil
}

func (fp *discovery) ListListeners(ctx context.Context) []*config.Listener {
	return fp.options.Listeners
}

func (fp *discovery) GetServiceRegistry(ctx context.Context) *config.Registry {
	return fp.options.Registry
}

func (fp *discovery) ListClusters(ctx context.Context, tenant string) ([]string, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	if cfg == nil || len(cfg.DataSourceClusters) == 0 {
		return nil, nil
	}

	ret := make([]string, 0, len(cfg.DataSourceClusters))
	for _, it := range cfg.DataSourceClusters {
		ret = append(ret, it.Name)
	}
	return ret, nil
}

func (fp *discovery) ListGroups(ctx context.Context, tenant, cluster string) ([]string, error) {
	bingo, err := fp.loadCluster(tenant, cluster)
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0, len(bingo.Groups))
	for _, it := range bingo.Groups {
		groups = append(groups, it.Name)
	}

	return groups, nil
}

func (fp *discovery) ListNodes(ctx context.Context, tenant, cluster, group string) ([]string, error) {
	bingo, ok := fp.loadGroup(tenant, cluster, group)
	if !ok {
		return nil, nil
	}

	nodes := make([]string, len(bingo.Nodes))
	copy(nodes, bingo.Nodes)

	return nodes, nil
}

func (fp *discovery) ListTables(ctx context.Context, tenant, cluster string) ([]string, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	shardingRule := cfg.ShardingRule
	tables := make([]string, 0, 4)

	for i := range shardingRule.Tables {
		db, tb, err := misc.ParseTable(shardingRule.Tables[i].Name)
		if err != nil {
			return nil, err
		}
		if db != cluster {
			continue
		}

		tables = append(tables, tb)
	}

	return tables, nil
}

func (fp *discovery) GetNode(ctx context.Context, tenant, cluster, group, node string) (*config.Node, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	var nodeId string

	bingo, ok := fp.loadGroup(tenant, cluster, group)
	if !ok {
		return nil, nil
	}

	for i := range bingo.Nodes {
		if bingo.Nodes[i] == node {
			nodeId = node
			break
		}
	}

	if nodeId == "" {
		return nil, nil
	}

	nodes, err := fp.loadNodes(op)
	if err != nil {
		return nil, err
	}

	return nodes[nodeId], nil
}

func (fp *discovery) GetSysDB(ctx context.Context, tenant string) (*config.Node, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	return cfg.SysDB, nil
}

func (fp *discovery) GetTable(ctx context.Context, tenant, cluster, tableName string) (*rule.VTable, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	table, ok := fp.loadTables(cluster, op)[tableName]
	if !ok {
		return nil, nil
	}

	return makeVTable(tableName, table)
}

func (fp *discovery) loadCluster(tenant, cluster string) (*config.DataSourceCluster, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, ErrorNoTenant
	}

	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	for _, it := range cfg.DataSourceClusters {
		if it.Name == cluster {
			return it, nil
		}
	}
	return nil, ErrorNoDataSourceCluster
}

func (fp *discovery) loadNodes(op config.Center) (config.Nodes, error) {
	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil, err
	}

	return cfg.Nodes, nil
}

func (fp *discovery) loadGroup(tenant, cluster, group string) (*config.Group, bool) {
	bingo, err := fp.loadCluster(tenant, cluster)
	if err != nil {
		return nil, false
	}
	for _, it := range bingo.Groups {
		if it.Name == group {
			return it, true
		}
	}
	return nil, false
}

func (fp *discovery) loadTables(cluster string, op config.Center) map[string]*config.Table {
	cfg, err := op.LoadAll(context.Background())
	if err != nil {
		return nil
	}

	var tables map[string]*config.Table
	for _, it := range cfg.ShardingRule.Tables {
		db, tb, err := misc.ParseTable(it.Name)
		if err != nil {
			log.Warnf("skip parsing table rule: %v", err)
			continue
		}
		if db != cluster {
			continue
		}
		if tables == nil {
			tables = make(map[string]*config.Table)
		}
		tables[tb] = it
	}
	return tables
}

func (fp *discovery) GetOptions() *config.BootOptions {
	return fp.options
}

func NewDiscovery(path string) Discovery {
	return &discovery{
		path:    path,
		centers: map[string]config.Center{},
	}
}
