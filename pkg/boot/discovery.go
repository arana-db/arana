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
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

import (
	"github.com/pkg/errors"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	rrule "github.com/arana-db/arana/pkg/runtime/rule"
	"github.com/arana-db/arana/pkg/util/file"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ Discovery = (*discovery)(nil)

var (
	_regexpTable     *regexp.Regexp
	_regexpTableOnce sync.Once
)

var (
	_regexpRuleExpr     *regexp.Regexp
	_regexpRuleExprSync sync.Once
)

func getTableRegexp() *regexp.Regexp {
	_regexpTableOnce.Do(func() {
		_regexpTable = regexp.MustCompile("([a-zA-Z0-9\\-_]+)\\.([a-zA-Z0-9\\\\-_]+)")
	})
	return _regexpTable
}

func getRuleExprRegexp() *regexp.Regexp {
	_regexpRuleExprSync.Do(func() {
		_regexpRuleExpr = regexp.MustCompile(`([a-zA-Z0-9_]+)\(\s*([0-9]|[1-9][0-9]+)?\s*\)`)
	})
	return _regexpRuleExpr
}

type discovery struct {
	path    string
	options *BootOptions
	c       *config.Center
}

func (fp *discovery) UpsertTenant(ctx context.Context, tenant string, body *TenantBody) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) RemoveTenant(ctx context.Context, tenant string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) UpsertCluster(ctx context.Context, tenant, cluster string, body *ClusterBody) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) RemoveCluster(ctx context.Context, tenant, cluster string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) UpsertNode(ctx context.Context, tenant, node string, body *NodeBody) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) RemoveNode(ctx context.Context, tenant, node string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) UpsertGroup(ctx context.Context, tenant, cluster, group string, body *GroupBody) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) RemoveGroup(ctx context.Context, tenant, cluster, group string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) BindNode(ctx context.Context, tenant, cluster, group, node string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) UnbindNode(ctx context.Context, tenant, cluster, group, node string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) UpsertTable(ctx context.Context, tenant, cluster, table string, body *TableBody) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) RemoveTable(ctx context.Context, tenant, cluster, table string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *discovery) Init(ctx context.Context) error {
	if err := fp.loadBootOptions(); err != nil {
		return err
	}

	if err := fp.initConfigCenter(); err != nil {
		return err
	}

	return nil
}

func (fp *discovery) loadBootOptions() error {
	content, err := ioutil.ReadFile(fp.path)
	if err != nil {
		err = errors.Wrap(err, "failed to load config")
		return err
	}

	if !file.IsYaml(fp.path) {
		err = errors.Errorf("invalid config file format: %s", filepath.Ext(fp.path))
		return err
	}

	var cfg BootOptions
	if err = yaml.Unmarshal(content, &cfg); err != nil {
		err = errors.Wrapf(err, "failed to unmarshal config")
		return err
	}

	fp.options = &cfg
	return nil
}

func (fp *discovery) initConfigCenter() error {
	c, err := config.NewCenter(*fp.options.Config)
	if err != nil {
		return err
	}

	fp.c = c

	return nil
}

func (fp *discovery) GetConfigCenter() *config.Center {
	return fp.c
}

func (fp *discovery) GetDataSourceCluster(ctx context.Context, cluster string) (*config.DataSourceCluster, error) {
	dataSourceCluster, ok := fp.loadCluster(cluster)
	if !ok {
		return nil, nil
	}
	return dataSourceCluster, nil
}

func (fp *discovery) GetCluster(ctx context.Context, cluster string) (*Cluster, error) {
	exist, ok := fp.loadCluster(cluster)
	if !ok {
		return nil, nil
	}

	return &Cluster{
		Tenant: exist.Tenant,
		Type:   exist.Type,
	}, nil
}

func (fp *discovery) ListTenants(ctx context.Context) ([]string, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	var tenants []string
	for _, it := range cfg.Data.Tenants {
		tenants = append(tenants, it.Name)
	}
	return tenants, nil
}

func (fp *discovery) GetTenant(ctx context.Context, tenant string) (*config.Tenant, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	for _, it := range cfg.Data.Tenants {
		if it.Name == tenant {
			return it, nil
		}
	}
	return nil, nil
}

func (fp *discovery) ListListeners(ctx context.Context) ([]*config.Listener, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	return cfg.Data.Listeners, nil
}

func (fp *discovery) ListFilters(ctx context.Context) ([]*config.Filter, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	return cfg.Data.Filters, nil
}

func (fp *discovery) ListClusters(ctx context.Context) ([]string, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	clusters := make([]string, 0, len(cfg.Data.DataSourceClusters))
	for _, it := range cfg.Data.DataSourceClusters {
		clusters = append(clusters, it.Name)
	}

	return clusters, nil
}

func (fp *discovery) ListGroups(ctx context.Context, cluster string) ([]string, error) {
	bingo, ok := fp.loadCluster(cluster)
	if !ok {
		return nil, nil
	}
	groups := make([]string, 0, len(bingo.Groups))
	for _, it := range bingo.Groups {
		groups = append(groups, it.Name)
	}

	return groups, nil
}

func (fp *discovery) ListNodes(ctx context.Context, cluster, group string) ([]string, error) {
	bingo, ok := fp.loadGroup(cluster, group)
	if !ok {
		return nil, nil
	}

	var nodes []string
	for _, it := range bingo.Nodes {
		nodes = append(nodes, it.Name)
	}

	return nodes, nil
}

func (fp *discovery) ListTables(ctx context.Context, cluster string) ([]string, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	var tables []string
	for tb := range fp.loadTables(cfg, cluster) {
		tables = append(tables, tb)
	}
	sort.Strings(tables)
	return tables, nil
}

func (fp *discovery) GetNode(ctx context.Context, cluster, group, node string) (*config.Node, error) {
	bingo, ok := fp.loadGroup(cluster, group)
	if !ok {
		return nil, nil
	}
	for _, it := range bingo.Nodes {
		if it.Name == node {
			return it, nil
		}
	}
	return nil, nil
}

func (fp *discovery) GetTable(ctx context.Context, cluster, tableName string) (*rule.VTable, error) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, err
	}

	table, ok := fp.loadTables(cfg, cluster)[tableName]
	if !ok {
		return nil, nil
	}

	var (
		vt                 rule.VTable
		topology           rule.Topology
		dbFormat, tbFormat string
		dbBegin, tbBegin   int
		dbEnd, tbEnd       int
	)

	if table.Topology != nil {
		if len(table.Topology.DbPattern) > 0 {
			if dbFormat, dbBegin, dbEnd, err = parseTopology(table.Topology.DbPattern); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		if len(table.Topology.TblPattern) > 0 {
			if tbFormat, tbBegin, tbEnd, err = parseTopology(table.Topology.TblPattern); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	topology.SetRender(getRender(dbFormat), getRender(tbFormat))

	var (
		keys                 map[string]struct{}
		dbSharder, tbSharder map[string]rule.ShardComputer
		dbSteps, tbSteps     map[string]int
	)
	for _, it := range table.DbRules {
		var shd rule.ShardComputer
		if shd, err = toSharder(it); err != nil {
			return nil, err
		}
		if dbSharder == nil {
			dbSharder = make(map[string]rule.ShardComputer)
		}
		if keys == nil {
			keys = make(map[string]struct{})
		}
		if dbSteps == nil {
			dbSteps = make(map[string]int)
		}
		dbSharder[it.Column] = shd
		keys[it.Column] = struct{}{}
		dbSteps[it.Column] = it.Step
	}

	for _, it := range table.TblRules {
		var shd rule.ShardComputer
		if shd, err = toSharder(it); err != nil {
			return nil, err
		}
		if tbSharder == nil {
			tbSharder = make(map[string]rule.ShardComputer)
		}
		if keys == nil {
			keys = make(map[string]struct{})
		}
		if tbSteps == nil {
			tbSteps = make(map[string]int)
		}
		tbSharder[it.Column] = shd
		keys[it.Column] = struct{}{}
		tbSteps[it.Column] = it.Step
	}

	for k := range keys {
		var (
			shd                    rule.ShardComputer
			dbMetadata, tbMetadata *rule.ShardMetadata
		)
		if shd, ok = dbSharder[k]; ok {
			dbMetadata = &rule.ShardMetadata{
				Computer: shd,
				Stepper:  rule.DefaultNumberStepper,
			}
			if s, ok := dbSteps[k]; ok && s > 0 {
				dbMetadata.Steps = s
			} else if dbBegin >= 0 && dbEnd >= 0 {
				dbMetadata.Steps = 1 + dbEnd - dbBegin
			}
		}
		if shd, ok = tbSharder[k]; ok {
			tbMetadata = &rule.ShardMetadata{
				Computer: shd,
				Stepper:  rule.DefaultNumberStepper,
			}
			if s, ok := tbSteps[k]; ok && s > 0 {
				tbMetadata.Steps = s
			} else if tbBegin >= 0 && tbEnd >= 0 {
				tbMetadata.Steps = 1 + tbEnd - tbBegin
			}
		}
		vt.SetShardMetadata(k, dbMetadata, tbMetadata)

		tpRes := make(map[int][]int)
		step := tbMetadata.Steps
		if dbMetadata.Steps > step {
			step = dbMetadata.Steps
		}
		rng, _ := tbMetadata.Stepper.Ascend(0, step)
		for rng.HasNext() {
			var (
				seed  = rng.Next()
				dbIdx = -1
				tbIdx = -1
			)
			if dbMetadata != nil {
				if dbIdx, err = dbMetadata.Computer.Compute(seed); err != nil {
					return nil, errors.WithStack(err)
				}
			}
			if tbMetadata != nil {
				if tbIdx, err = tbMetadata.Computer.Compute(seed); err != nil {
					return nil, errors.WithStack(err)
				}
			}
			tpRes[dbIdx] = append(tpRes[dbIdx], tbIdx)
		}

		for dbIndex, tbIndexes := range tpRes {
			topology.SetTopology(dbIndex, tbIndexes...)
		}
	}

	if table.AllowFullScan {
		vt.SetAllowFullScan(true)
	}
	if table.Sequence != nil {
		vt.SetAutoIncrement(&rule.AutoIncrement{
			Type:   table.Sequence.Type,
			Option: table.Sequence.Option,
		})
	}

	// TODO: process attributes
	_ = table.Attributes["sql_max_limit"]

	vt.SetTopology(&topology)
	vt.SetName(tableName)

	return &vt, nil
}

func (fp *discovery) loadCluster(cluster string) (*config.DataSourceCluster, bool) {
	cfg, err := fp.c.Load()
	if err != nil {
		return nil, false
	}

	for _, it := range cfg.Data.DataSourceClusters {
		if it.Name == cluster {
			return it, true
		}
	}
	return nil, false
}

func (fp *discovery) loadGroup(cluster, group string) (*config.Group, bool) {
	bingo, ok := fp.loadCluster(cluster)
	if !ok {
		return nil, false
	}
	for _, it := range bingo.Groups {
		if it.Name == group {
			return it, true
		}
	}
	return nil, false
}

func (fp *discovery) loadTables(cfg *config.Configuration, cluster string) map[string]*config.Table {
	var tables map[string]*config.Table
	for _, it := range cfg.Data.ShardingRule.Tables {
		db, tb, err := parseTable(it.Name)
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

var (
	_regexpTopology     *regexp.Regexp
	_regexpTopologyOnce sync.Once
)

func getTopologyRegexp() *regexp.Regexp {
	_regexpTopologyOnce.Do(func() {
		_regexpTopology = regexp.MustCompile(`\${(?P<begin>\d+)\.{2,}(?P<end>\d+)}`)
	})
	return _regexpTopology
}

func parseTopology(input string) (format string, begin, end int, err error) {
	mats := getTopologyRegexp().FindAllStringSubmatch(input, -1)

	if len(mats) < 1 {
		format = input
		begin = -1
		end = -1
		return
	}

	if len(mats) > 1 {
		err = errors.Errorf("invalid topology expression: %s", input)
		return
	}

	var beginStr, endStr string
	for i := 1; i < len(mats[0]); i++ {
		switch getTopologyRegexp().SubexpNames()[i] {
		case "begin":
			beginStr = mats[0][i]
		case "end":
			endStr = mats[0][i]
		}
	}

	if len(beginStr) != len(endStr) {
		err = errors.Errorf("invalid topology expression: %s", input)
		return
	}

	format = getTopologyRegexp().ReplaceAllString(input, fmt.Sprintf(`%%0%dd`, len(beginStr)))
	begin, _ = strconv.Atoi(strings.TrimLeft(beginStr, "0"))
	end, _ = strconv.Atoi(strings.TrimLeft(endStr, "0"))
	return
}

func toSharder(input *config.Rule) (rule.ShardComputer, error) {
	var (
		computer rule.ShardComputer
		mod      int
		err      error
	)

	if mat := getRuleExprRegexp().FindStringSubmatch(input.Expr); len(mat) == 3 {
		mod, _ = strconv.Atoi(mat[2])
	}

	switch rrule.ShardType(input.Type) {
	case rrule.ModShard:
		computer = rrule.NewModShard(mod)
	case rrule.HashMd5Shard:
		computer = rrule.NewHashMd5Shard(mod)
	case rrule.HashBKDRShard:
		computer = rrule.NewHashBKDRShard(mod)
	case rrule.HashCrc32Shard:
		computer = rrule.NewHashCrc32Shard(mod)
	case rrule.FunctionExpr:
		computer, err = rrule.NewExprShardComputer(input.Expr, input.Column)
	case rrule.ScriptExpr:
		computer, err = rrule.NewJavascriptShardComputer(input.Expr)
	default:
		panic(fmt.Errorf("error config, unsupport shard type: %s", input.Type))
	}
	return computer, err
}

func getRender(format string) func(int) string {
	if strings.ContainsRune(format, '%') {
		return func(i int) string {
			return fmt.Sprintf(format, i)
		}
	}
	return func(i int) string {
		return format
	}
}

func parseTable(input string) (db, tbl string, err error) {
	mat := getTableRegexp().FindStringSubmatch(input)
	if len(mat) < 1 {
		err = errors.Errorf("invalid table name: %s", input)
		return
	}
	db = mat[1]
	tbl = mat[2]
	return
}

func NewDiscovery(path string) Discovery {
	return &discovery{
		path: path,
	}
}
