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

package config_test

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/testdata"
)

var FakeConfigPath = testdata.Path("fake_config.yaml")

func TestMetadataConf(t *testing.T) {
	conf, err := config.Load(FakeConfigPath)
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	assert.Equal(t, "ConfigMap", conf.Kind)
	assert.Equal(t, "1.0", conf.APIVersion)
	expectMetadata := map[string]interface{}{
		"name": "arana-config",
	}
	assert.Equal(t, expectMetadata, conf.Metadata)
}

func TestValidate(t *testing.T) {
	conf, err := config.Load(FakeConfigPath)
	assert.NoError(t, err)
	assert.NotNil(t, conf)

	err = config.Validate(conf)
	assert.NoError(t, err)
}

func TestDataSourceClustersConf(t *testing.T) {
	conf, err := config.Load(FakeConfigPath)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, conf)

	assert.Equal(t, 1, len(conf.Data.Tenants[0].DataSourceClusters))
	dataSourceCluster := conf.Data.Tenants[0].DataSourceClusters[0]
	assert.Equal(t, "employees", dataSourceCluster.Name)
	assert.Equal(t, config.DBMySQL, dataSourceCluster.Type)
	assert.Equal(t, -1, dataSourceCluster.SqlMaxLimit)
	assert.Equal(t, "arana", conf.Data.Tenants[0].Name)

	assert.Equal(t, 4, len(dataSourceCluster.Groups))
	group := dataSourceCluster.Groups[0]
	assert.Equal(t, "employees_0000", group.Name)
	assert.Equal(t, 2, len(group.Nodes))
	node := conf.Data.Tenants[0].Nodes["node0"]
	assert.Equal(t, "arana-mysql", node.Host)
	assert.Equal(t, 3306, node.Port)
	assert.Equal(t, "root", node.Username)
	assert.Equal(t, "123456", node.Password)
	assert.Equal(t, "employees_0000", node.Database)
	assert.Equal(t, "r10w10", node.Weight)
	// assert.Len(t, node.Labels, 1)
	// assert.NotNil(t, node.ConnProps)
}

func TestShardingRuleConf(t *testing.T) {
	conf, err := config.Load(FakeConfigPath)
	assert.NoError(t, err)
	assert.NotEqual(t, nil, conf)

	assert.NotNil(t, conf.Data.Tenants[0].ShardingRule)
	assert.Equal(t, 1, len(conf.Data.Tenants[0].ShardingRule.Tables))
	table := conf.Data.Tenants[0].ShardingRule.Tables[0]
	assert.Equal(t, "employees.student", table.Name)
	assert.Equal(t, true, table.AllowFullScan)

	assert.Len(t, table.DbRules, 1)
	assert.Equal(t, "uid", table.DbRules[0].Column)
	assert.Equal(t, "parseInt($value % 32 / 8)", table.DbRules[0].Expr)

	assert.Len(t, table.TblRules, 1)
	assert.Equal(t, "uid", table.TblRules[0].Column)
	assert.Equal(t, "$value % 32", table.TblRules[0].Expr)

	assert.Equal(t, "employees_${0000..0003}", table.Topology.DbPattern)
	assert.Equal(t, "student_${0000..0031}", table.Topology.TblPattern)
	// assert.Equal(t, "employee_0000", table.ShadowTopology.DbPattern)
	// assert.Equal(t, "__test_student_${0000...0007}", table.ShadowTopology.TblPattern)
	assert.Len(t, table.Attributes, 1)
}

func TestUnmarshalTextForProtocolTypeNil(t *testing.T) {
	var protocolType config.ProtocolType
	text := []byte("http")
	err := protocolType.UnmarshalText(text)
	assert.Nil(t, err)
	assert.Equal(t, config.Http, protocolType)
}

func TestUnmarshalTextForUnrecognizedProtocolType(t *testing.T) {
	protocolType := config.Http
	text := []byte("PostgreSQL")
	err := protocolType.UnmarshalText(text)
	assert.Error(t, err)
}

func TestUnmarshalText(t *testing.T) {
	protocolType := config.Http
	text := []byte("mysql")
	err := protocolType.UnmarshalText(text)
	assert.Nil(t, err)
	assert.Equal(t, config.MySQL, protocolType)
}
