//
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

package config

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

const configPath = "./testdata/config.yaml"

func TestMetadataConf(t *testing.T) {
	conf := LoadV2(configPath)
	assert.NotNil(t, conf)

	assert.Equal(t, "ConfigMap", conf.Kind)
	assert.Equal(t, "1.0", conf.APIVersion)
	assert.Equal(t, "arana-config", conf.Metadata.Name)
}

func TestListenersConf(t *testing.T) {
	conf := LoadV2(configPath)
	assert.NotNil(t, conf)

	assert.Equal(t, 1, len(conf.Data.Listeners))
	dataListeners := conf.Data.Listeners[0]
	assert.Equal(t, "mysql", dataListeners.ProtocolType)
	assert.Equal(t, "0.0.0.0", dataListeners.SocketAddress.Address)
	assert.Equal(t, 13306, dataListeners.SocketAddress.Port)
	assert.NotNil(t, dataListeners.Config)
	assert.Equal(t, "root", dataListeners.Config.Users[0].Username)
	assert.Equal(t, "123456", dataListeners.Config.Users[0].Password)
	assert.Equal(t, "5.7.0", dataListeners.Config.ServerVersion)
	assert.Equal(t, "redirect", dataListeners.Executor)
}

func TestExecutorsConf(t *testing.T) {
	conf := LoadV2(configPath)
	assert.NotNil(t, conf)

	assert.Equal(t, 1, len(conf.Data.Executors))
	dataExecutors := conf.Data.Executors[0]
	assert.Equal(t, "redirect", dataExecutors.Name)
	assert.Equal(t, "singledb", dataExecutors.Mode)
	assert.NotNil(t, dataExecutors.DataSources)
	assert.Equal(t, "employees", dataExecutors.DataSources[0].Master)
}

func TestFiltersConf(t *testing.T) {
	// todo
}

func TestDataSourceClustersConf(t *testing.T) {
	conf := LoadV2(configPath)
	assert.NotEqual(t, nil, conf)

	assert.Equal(t, 1, len(conf.Data.DataSourceClusters))
	dataSourceCluster := conf.Data.DataSourceClusters[0]
	assert.Equal(t, "employee", dataSourceCluster.Name)
	assert.Equal(t, DBMysql, dataSourceCluster.Type)
	assert.Equal(t, -1, dataSourceCluster.SqlMaxLimit)
	assert.Equal(t, "dksl", dataSourceCluster.Tenant)
	assert.NotNil(t, dataSourceCluster.ConnProps)
	assert.Equal(t, 20, dataSourceCluster.ConnProps.MaxCapacity)
	assert.Equal(t, 60, dataSourceCluster.ConnProps.IdleTimeout)

	assert.Equal(t, 1, len(dataSourceCluster.Groups))
	group := dataSourceCluster.Groups[0]
	assert.Equal(t, "employee_1", group.Name)
	assert.Equal(t, 1, len(group.AtomDbs))
	atomDb := group.AtomDbs[0]
	assert.Equal(t, "127.0.0.1", atomDb.Host)
	assert.Equal(t, 3306, atomDb.Port)
	assert.Equal(t, "root", atomDb.Username)
	assert.Equal(t, "123456", atomDb.Password)
	assert.Equal(t, "employees_0001", atomDb.Database)
	assert.Equal(t, Master, atomDb.Role)
	assert.Equal(t, "r0w10", atomDb.Weight)
	assert.NotNil(t, atomDb.DsnProps)
	assert.Equal(t, "1s", atomDb.DsnProps.ReadTimeout)
	assert.Equal(t, "1s", atomDb.DsnProps.WriteTimeout)
	assert.Equal(t, true, atomDb.DsnProps.ParseTime)
	assert.Equal(t, "Local", atomDb.DsnProps.Loc)
	assert.Equal(t, "utf8mb4,utf8", atomDb.DsnProps.Charset)
}

func TestShardingRuleConf(t *testing.T) {
	conf := LoadV2(configPath)
	assert.NotEqual(t, nil, conf)

	assert.NotNil(t, conf.Data.ShardingRule)
	assert.Equal(t, 1, len(conf.Data.ShardingRule.Tables))
	table := conf.Data.ShardingRule.Tables[0]
	assert.Equal(t, table.Name, "employee.student")
	assert.Equal(t, table.AllowFullScan, true)

	assert.Equal(t, 2, len(table.ActualDataNodes))
	assert.Equal(t, "employee_tmall.student", table.ActualDataNodes[0])
	assert.Equal(t, "employee_taobao.student", table.ActualDataNodes[1])

	assert.Equal(t, 2, len(table.DbRules))
	assert.Equal(t, "student_id", table.DbRules[0].Column)
	assert.Equal(t, "modShard(3)", table.DbRules[0].Expr)
	assert.Equal(t, "student_num", table.DbRules[1].Column)
	assert.Equal(t, "hashMd5Shard(3)", table.DbRules[1].Expr)

	assert.Equal(t, 1, len(table.TblRules))
	assert.Equal(t, "student_id", table.TblRules[0].Column)
	assert.Equal(t, "modShard(3)", table.TblRules[0].Expr)

	assert.NotNil(t, table.TblProps)
	assert.Equal(t, -1, table.TblProps.SqlMaxLimit)

	assert.Equal(t, "employee_${0000...0002}", table.Topology.DbPattern)
	assert.Equal(t, "student_${0000...0008}", table.Topology.TblPattern)
	assert.Equal(t, "employee_${0000...0002}", table.ShadowTopology.DbPattern)
	assert.Equal(t, "student_${0000...0008}", table.ShadowTopology.TblPattern)
}
