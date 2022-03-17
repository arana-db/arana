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
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	rrule "github.com/arana-db/arana/pkg/runtime/rule"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ Discovery = (*fileDiscovery)(nil)

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

type fileDiscovery struct {
	sync.Once
	path string
	c    *config.Configuration
}

func (fp *fileDiscovery) GetCluster(ctx context.Context, cluster string) (*Cluster, error) {
	exist, ok := fp.loadCluster(cluster)
	if !ok {
		return nil, nil
	}

	return &Cluster{
		Tenant: exist.Tenant,
		Type:   exist.Type,
	}, nil
}

func (fp *fileDiscovery) Init(ctx context.Context) (err error) {
	fp.Do(func() {
		fp.c, err = config.ParseV2(fp.path)
	})
	return
}

func (fp *fileDiscovery) ListTenants(ctx context.Context) ([]string, error) {
	var tenants []string
	for _, it := range fp.c.Data.Tenants {
		tenants = append(tenants, it.Name)
	}
	return tenants, nil
}

func (fp *fileDiscovery) GetTenant(ctx context.Context, tenant string) (*config.Tenant, error) {
	for _, it := range fp.c.Data.Tenants {
		if it.Name == tenant {
			return it, nil
		}
	}
	return nil, nil
}

func (fp *fileDiscovery) ListListeners(ctx context.Context) ([]*config.Listener, error) {
	return fp.c.Data.Listeners, nil
}

func (fp *fileDiscovery) ListFilters(ctx context.Context) ([]*config.Filter, error) {
	return fp.c.Data.Filters, nil
}

func (fp *fileDiscovery) ListClusters(ctx context.Context) ([]string, error) {
	clusters := make([]string, 0, len(fp.c.Data.DataSourceClusters))
	for _, it := range fp.c.Data.DataSourceClusters {
		clusters = append(clusters, it.Name)
	}

	return clusters, nil
}

func (fp *fileDiscovery) ListGroups(ctx context.Context, cluster string) ([]string, error) {
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

func (fp *fileDiscovery) ListNodes(ctx context.Context, cluster, group string) ([]string, error) {
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

func (fp *fileDiscovery) ListTables(ctx context.Context, cluster string) ([]string, error) {
	var tables []string
	for _, it := range fp.loadTables(cluster) {
		_, tb, _ := parseTable(it.Name)
		tables = append(tables, tb)
	}
	sort.Strings(tables)
	return tables, nil
}

func (fp *fileDiscovery) GetNode(ctx context.Context, cluster, group, node string) (*config.Node, error) {
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

func (fp *fileDiscovery) GetTable(ctx context.Context, cluster, table string) (*rule.VTable, error) {
	exist, ok := fp.loadTables(cluster)[table]
	if !ok {
		return nil, nil
	}
	var vt rule.VTable

	var (
		topology rule.Topology
		err      error

		dbFormat, tbFormat string
		dbBegin, tbBegin   int
		dbEnd, tbEnd       int
	)

	if exist.Topology != nil {
		if len(exist.Topology.DbPattern) > 0 {
			if dbFormat, dbBegin, dbEnd, err = parseTopology(exist.Topology.DbPattern); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		if len(exist.Topology.TblPattern) > 0 {
			if tbFormat, tbBegin, tbEnd, err = parseTopology(exist.Topology.TblPattern); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	topology.SetRender(getRender(dbFormat), getRender(tbFormat))

	var (
		keys                 map[string]struct{}
		dbSharder, tbSharder map[string]rule.ShardComputer
	)
	for _, it := range exist.DbRules {
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
		dbSharder[it.Column] = shd
		keys[it.Column] = struct{}{}
	}

	for _, it := range exist.TblRules {
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
		tbSharder[it.Column] = shd
		keys[it.Column] = struct{}{}
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
			if dbBegin >= 0 && dbEnd >= 0 {
				dbMetadata.Steps = 1 + dbEnd - dbBegin
			}
		}
		if shd, ok = tbSharder[k]; ok {
			tbMetadata = &rule.ShardMetadata{
				Computer: shd,
				Stepper:  rule.DefaultNumberStepper,
			}
			if tbBegin >= 0 && tbEnd >= 0 {
				tbMetadata.Steps = 1 + tbEnd - tbBegin
			}
		}
		vt.SetShardMetadata(k, dbMetadata, tbMetadata)

		tpRes := make(map[int][]int)
		rng, _ := tbMetadata.Stepper.Ascend(0, tbMetadata.Steps)
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

	if exist.AllowFullScan {
		vt.SetAllowFullScan(true)
	}

	// TODO: process attributes
	_ = exist.Attributes["sql_max_limit"]

	vt.SetTopology(&topology)

	return &vt, nil
}

func (fp *fileDiscovery) loadCluster(cluster string) (*config.DataSourceCluster, bool) {
	for _, it := range fp.c.Data.DataSourceClusters {
		if it.Name == cluster {
			return it, true
		}
	}
	return nil, false
}

func (fp *fileDiscovery) loadGroup(cluster, group string) (*config.Group, bool) {
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

func (fp *fileDiscovery) loadTables(cluster string) map[string]*config.Table {
	var tables map[string]*config.Table
	for _, it := range fp.c.Data.ShardingRule.Tables {
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
		_regexpTopology = regexp.MustCompile(`\${(?P<begin>[0-9]+)\.\.\.(?P<end>[0-9]+)}`)
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

	var (
		beginStr, endStr string
	)
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
	mat := getRuleExprRegexp().FindStringSubmatch(input.Expr)
	if len(mat) != 3 {
		return nil, errors.Errorf("invalid shard rule: %s", input.Expr)
	}

	var (
		computer rule.ShardComputer
		method   = mat[1]
		n, _     = strconv.Atoi(mat[2])
	)

	switch method {
	case string(rrule.ModShard):
		computer = rrule.NewModShard(n)
	case string(rrule.HashMd5Shard):
		computer = rrule.NewHashMd5Shard(n)
	case string(rrule.HashBKDRShard):
		computer = rrule.NewHashBKDRShard(n)
	case string(rrule.HashCrc32Shard):
		computer = rrule.NewHashCrc32Shard(n)
	default:
		return nil, errors.Errorf("invalid shard rule: %s", input.Expr)
	}
	return computer, nil
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

func NewFileProvider(path string) Discovery {
	return &fileDiscovery{
		path: path,
	}
}
