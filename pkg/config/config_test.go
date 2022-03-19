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

import (
	"github.com/arana-db/arana/testdata"
)

var fakeConfigPath = testdata.Path("fake_config.yaml")

func TestMetadataConf(t *testing.T) {
	conf, err := LoadV2(fakeConfigPath)
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	assert.Equal(t, "Configuration", conf.Kind)
	assert.Equal(t, "1.0", conf.APIVersion)
	expectMetadata := map[string]interface{}{
		"name": "arana-config",
	}
	assert.Equal(t, expectMetadata, conf.Metadata)
}

func TestDataSourceClustersConf(t *testing.T) {
	conf, err := LoadV2(fakeConfigPath)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, conf)

	assert.Equal(t, 1, len(conf.Data.DataSourceClusters))
	dataSourceCluster := conf.Data.DataSourceClusters[0]
	assert.Equal(t, "employee", dataSourceCluster.Name)
	assert.Equal(t, DBMysql, dataSourceCluster.Type)
	assert.Equal(t, -1, dataSourceCluster.SqlMaxLimit)
	assert.Equal(t, "arana", dataSourceCluster.Tenant)
	assert.NotNil(t, dataSourceCluster.ConnProps)
	assert.Equal(t, 10, dataSourceCluster.ConnProps.Capacity)
	assert.Equal(t, 20, dataSourceCluster.ConnProps.MaxCapacity)
	assert.Equal(t, 60, dataSourceCluster.ConnProps.IdleTimeout)

	assert.Equal(t, 1, len(dataSourceCluster.Groups))
	group := dataSourceCluster.Groups[0]
	assert.Equal(t, "employee_0000", group.Name)
	assert.Equal(t, 1, len(group.Nodes))
	node := group.Nodes[0]
	assert.Equal(t, "127.0.0.1", node.Host)
	assert.Equal(t, 3306, node.Port)
	assert.Equal(t, "root", node.Username)
	assert.Equal(t, "123456", node.Password)
	assert.Equal(t, "employees_0001", node.Database)
	assert.Equal(t, "r10w10", node.Weight)
	assert.Len(t, node.Labels, 1)
	assert.NotNil(t, node.ConnProps)
}

func TestShardingRuleConf(t *testing.T) {
	conf, err := LoadV2(fakeConfigPath)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, conf)

	assert.NotNil(t, conf.Data.ShardingRule)
	assert.Equal(t, 1, len(conf.Data.ShardingRule.Tables))
	table := conf.Data.ShardingRule.Tables[0]
	assert.Equal(t, table.Name, "employee.student")
	assert.Equal(t, table.AllowFullScan, true)

	assert.Len(t, table.DbRules, 1)
	assert.Equal(t, "student_id", table.DbRules[0].Column)
	assert.Equal(t, "modShard(3)", table.DbRules[0].Expr)

	assert.Len(t, table.TblRules, 1)
	assert.Equal(t, "student_id", table.TblRules[0].Column)
	assert.Equal(t, "modShard(8)", table.TblRules[0].Expr)

	assert.Equal(t, "employee_0000", table.Topology.DbPattern)
	assert.Equal(t, "student_${0000...0007}", table.Topology.TblPattern)
	assert.Equal(t, "employee_0000", table.ShadowTopology.DbPattern)
	assert.Equal(t, "__test_student_${0000...0007}", table.ShadowTopology.TblPattern)
	assert.Len(t, table.Attributes, 2)
}

func TestUnmarshalTextForProtocolTypeNil(t *testing.T) {
	var protocolType ProtocolType
	var text = []byte("http")
	err := protocolType.UnmarshalText(text)
	assert.Nil(t, err)
	assert.Equal(t, Http, protocolType)
}

func TestUnmarshalTextForUnrecognizedProtocolType(t *testing.T) {
	var protocolType = Http
	var text = []byte("PostgreSQL")
	err := protocolType.UnmarshalText(text)
	assert.Error(t, err)
}

func TestUnmarshalText(t *testing.T) {
	var protocolType = Http
	var text = []byte("mysql")
	err := protocolType.UnmarshalText(text)
	assert.Nil(t, err)
	assert.Equal(t, Mysql, protocolType)
}
