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
	"fmt"
	"io"
	"os"
	"testing"
	"time"
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

	assert.Len(t, table.DbRules, 1)
	assert.Equal(t, "uid", table.DbRules[0].Columns[0].Name)
	assert.Equal(t, "parseInt($value % 32 / 8)", table.DbRules[0].Expr)

	assert.Len(t, table.TblRules, 1)
	assert.Equal(t, "uid", table.TblRules[0].Columns[0].Name)
	assert.Equal(t, "$value % 32", table.TblRules[0].Expr)

	assert.Equal(t, "employees_${0000..0003}", table.Topology.DbPattern)
	assert.Equal(t, "student_${0000..0031}", table.Topology.TblPattern)
	// assert.Equal(t, "employee_0000", table.ShadowTopology.DbPattern)
	// assert.Equal(t, "__test_student_${0000...0007}", table.ShadowTopology.TblPattern)
	assert.Len(t, table.Attributes, 2)
	assert.Equal(t, "true", table.Attributes["allow_full_scan"])
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

func TestParametersMap_LowerCaseFirstLetter(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name string
		pm   config.ParametersMap
		args args
		want string
	}{
		{
			name: "test lower case first letter",
			pm:   config.ParametersMap{},
			args: args{
				str: "Arana",
			},
			want: "arana",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.pm.LowerCaseFirstLetter(tt.args.str), "LowerCaseFirstLetter(%v)", tt.args.str)
		})
	}
}

func TestDecoder_Decode(t *testing.T) {
	type fields struct {
		reader io.Reader
	}
	type args struct {
		v interface{}
	}

	f, err := os.Open(FakeConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = f.Close()
	}()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test decode",
			args: args{
				v: &config.Configuration{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := config.NewDecoder(f).Decode(tt.args.v); err != nil {
				t.Errorf("Decode() error = %v", err)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *config.Configuration
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test load",
			args: args{
				path: FakeConfigPath,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.Load(tt.args.path)
			if err != nil {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			t.Logf("got: %+v", got)
		})
	}
}

func TestTenant_Empty(t1 *testing.T) {
	type fields struct {
		Spec               config.Spec
		Name               string
		Users              []*config.User
		SysDB              *config.Node
		DataSourceClusters []*config.DataSourceCluster
		ShardingRule       *config.ShardingRule
		ShadowRule         *config.ShadowRule
		Nodes              map[string]*config.Node
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "test empty",
			fields: fields{
				Spec:               config.Spec{},
				Name:               "",
				Users:              nil,
				SysDB:              nil,
				DataSourceClusters: nil,
				ShardingRule:       nil,
				ShadowRule:         nil,
				Nodes:              nil,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &config.Tenant{
				Spec:               tt.fields.Spec,
				Name:               tt.fields.Name,
				Users:              tt.fields.Users,
				SysDB:              tt.fields.SysDB,
				DataSourceClusters: tt.fields.DataSourceClusters,
				ShardingRule:       tt.fields.ShardingRule,
				ShadowRule:         tt.fields.ShadowRule,
				Nodes:              tt.fields.Nodes,
			}
			assert.Equalf(t1, tt.want, t.Empty(), "Empty()")
		})
	}
}

func TestNode_GetReadAndWriteWeight(t *testing.T) {
	type fields *config.Node

	node := &config.Node{
		Name:      "arana-node-1",
		Host:      "arana-mysql",
		Port:      3306,
		Username:  "root",
		Password:  "123456",
		Database:  "employees",
		ConnProps: nil,
		Weight:    "r10w10",
		Labels:    nil,
	}

	tests := []struct {
		name    string
		fields  fields
		want    int
		want1   int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:   "test get read and write weight",
			fields: node,
			want:   10,
			want1:  10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := node.GetReadAndWriteWeight()
			if err != nil {
				t.Errorf("GetReadAndWriteWeight() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want1, got1)
		})
	}
}

func TestGetConnPropCapacity(t *testing.T) {
	type args struct {
		connProps    map[string]interface{}
		defaultValue int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test get conn prop capacity",
			args: args{
				connProps: map[string]interface{}{
					"capacity": 10,
				},
				defaultValue: 1,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, config.GetConnPropCapacity(tt.args.connProps, tt.args.defaultValue), "GetConnPropCapacity(%v, %v)", tt.args.connProps, tt.args.defaultValue)
		})
	}
}

func TestGetConnPropMaxCapacity(t *testing.T) {
	type args struct {
		connProps    map[string]interface{}
		defaultValue int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test get conn prop max capacity",
			args: args{
				connProps: map[string]interface{}{
					"maxCapacity": 10,
				},
				defaultValue: 1,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, config.GetConnPropMaxCapacity(tt.args.connProps, tt.args.defaultValue), "GetConnPropMaxCapacity(%v, %v)", tt.args.connProps, tt.args.defaultValue)
		})
	}
}

func TestGetConnPropIdleTime(t *testing.T) {
	type args struct {
		connProps    map[string]interface{}
		defaultValue time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "test get conn prop idle time",
			args: args{
				connProps: map[string]interface{}{
					"idle_time": "10s",
				},
				defaultValue: 1 * time.Second,
			},
			want: 10 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, config.GetConnPropIdleTime(tt.args.connProps, tt.args.defaultValue), "GetConnPropIdleTime(%v, %v)", tt.args.connProps, tt.args.defaultValue)
		})
	}
}

func TestListener_String(t *testing.T) {
	type fields struct {
		ProtocolType  string
		SocketAddress *config.SocketAddress
		ServerVersion string
	}

	socketAddr := fmt.Sprintf("%s:%d", "127.0.0.1", 3306)
	want := fmt.Sprintf("Listener protocol_type:%s, socket_address:%s, server_version:%s", "mysql", socketAddr, "5.7.25")

	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "test listener string",
			fields: fields{
				ProtocolType: "mysql",
				SocketAddress: &config.SocketAddress{
					Address: "127.0.0.1",
					Port:    3306,
				},
				ServerVersion: "5.7.25",
			},
			want: want,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &config.Listener{
				ProtocolType:  tt.fields.ProtocolType,
				SocketAddress: tt.fields.SocketAddress,
				ServerVersion: tt.fields.ServerVersion,
			}
			assert.Equalf(t, tt.want, l.String(), "String()")
		})
	}
}
