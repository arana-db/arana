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

package file

import (
	"reflect"
	"testing"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/testdata"
)

var FakeConfigPath = testdata.Path("fake_config.yaml")
var EmptyConfigPath = testdata.Path("fake_empty_config.yaml")

var jsonConfig = `{
    "kind":"ConfigMap",
    "apiVersion":"1.0",
    "metadata":{
        "name":"arana-config"
    },
    "data":{
        "listeners":[
            {
                "protocol_type":"mysql",
                "socket_address":{
                    "address":"0.0.0.0",
                    "port":13306
                },
                "server_version":"5.7.0"
            }
        ],
        "tenants":[
            {
                "name":"arana",
                "users":[
                    {
                        "username":"arana",
                        "password":"123456"
                    },
                    {
                        "username":"dksl",
                        "password":"123456"
                    }
                ]
            }
        ],
        "clusters":[
            {
                "name":"employees",
                "type":"mysql",
                "sql_max_limit":-1,
                "tenant":"arana",
                "parameters":{
                    "max_allowed_packet":"256M"
                },
                "groups":[
                    {
                        "name":"employees_0000",
                        "nodes":[
                            {
                                "name":"node0",
                                "host":"arana-mysql",
                                "port":3306,
                                "username":"root",
                                "password":"123456",
                                "database":"employees_0000",
                                "parameters":null,
                                "weight":"r10w10"
                            },
                            {
                                "name":"node0_r_0",
                                "host":"arana-mysql",
                                "port":3306,
                                "username":"root",
                                "password":"123456",
                                "database":"employees_0000_r",
                                "parameters":null,
                                "weight":"r0w0"
                            }
                        ]
                    },
                    {
                        "name":"employees_0001",
                        "nodes":[
                            {
                                "name":"node1",
                                "host":"arana-mysql",
                                "port":3306,
                                "username":"root",
                                "password":"123456",
                                "database":"employees_0001",
                                "parameters":null,
                                "weight":"r10w10"
                            }
                        ]
                    },
                    {
                        "name":"employees_0002",
                        "nodes":[
                            {
                                "name":"node2",
                                "host":"arana-mysql",
                                "port":3306,
                                "username":"root",
                                "password":"123456",
                                "database":"employees_0002",
                                "parameters":null,
                                "weight":"r10w10"
                            }
                        ]
                    },
                    {
                        "name":"employees_0003",
                        "nodes":[
                            {
                                "name":"node3",
                                "host":"arana-mysql",
                                "port":3306,
                                "username":"root",
                                "password":"123456",
                                "database":"employees_0003",
                                "parameters":null,
                                "weight":"r10w10"
                            }
                        ]
                    }
                ]
            }
        ],
        "sharding_rule":{
            "tables":[
                {
                    "name":"employees.student",
                    "sequence":{
                        "type":"snowflake",
                        "option":null
                    },
                    "allow_full_scan":true,
                    "db_rules":[
                        {
                            "column":"uid",
                            "type":"scriptExpr",
                            "expr":"parseInt($value % 32 / 8)",
                            "step":0
                        }
                    ],
                    "tbl_rules":[
                        {
                            "column":"uid",
                            "type":"scriptExpr",
                            "expr":"$value % 32",
                            "step":32
                        }
                    ],
                    "topology":{
                        "db_pattern":"employees_${0000..0003}",
                        "tbl_pattern":"student_${0000..0031}"
                    },
                    "shadow_topology":null,
                    "attributes":{
                        "sqlMaxLimit":"-1"
                    }
                }
            ]
        }
    }
}`

var yamlConfig = `
kind: ConfigMap
apiVersion: "1.0"
metadata:
  name: arana-config
data:
  listeners:
    - protocol_type: mysql
      server_version: 5.7.0
      socket_address:
        address: 0.0.0.0
        port: 13306

  tenants:
    - name: arana
      users:
        - username: arana
          password: "123456"
        - username: dksl
          password: "123456"

  clusters:
    - name: employees
      type: mysql
      sql_max_limit: -1
      tenant: arana
      parameters:
        max_allowed_packet: 256M
      groups:
        - name: employees_0000
          nodes:
            - name: node0
              host: arana-mysql
              port: 3306
              username: root
              password: "123456"
              database: employees_0000
              weight: r10w10
              parameters:
            - name: node0_r_0
              host: arana-mysql
              port: 3306
              username: root
              password: "123456"
              database: employees_0000_r
              weight: r0w0
              parameters:
        - name: employees_0001
          nodes:
            - name: node1
              host: arana-mysql
              port: 3306
              username: root
              password: "123456"
              database: employees_0001
              weight: r10w10
              parameters:
        - name: employees_0002
          nodes:
            - name: node2
              host: arana-mysql
              port: 3306
              username: root
              password: "123456"
              database: employees_0002
              weight: r10w10
              parameters:
        - name: employees_0003
          nodes:
            - name: node3
              host: arana-mysql
              port: 3306
              username: root
              password: "123456"
              database: employees_0003
              weight: r10w10
              parameters:
  sharding_rule:
    tables:
      - name: employees.student
        allow_full_scan: true
        db_rules:
          - column: uid
            type: scriptExpr
            expr: parseInt($value % 32 / 8)
        tbl_rules:
          - column: uid
            type: scriptExpr
            expr: $value % 32
            step: 32
        topology:
          db_pattern: employees_${0000..0003}
          tbl_pattern: student_${0000..0031}
        attributes:
          sqlMaxLimit: -1
`

func Test_storeOperate_Close(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"Close", fields{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			if err := s.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_storeOperate_Get(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	type args struct {
		key config.PathKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"Get",
			fields{nil, map[config.PathKey]string{"/arana-db/config/data/dataSourceClusters": "test"}},
			args{"/arana-db/config/data/dataSourceClusters"},
			[]byte{0x74, 0x65, 0x73, 0x74},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			got, err := s.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storeOperate_Init(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	type args struct {
		options map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Init_1",
			fields{},
			args{map[string]interface{}{"a": "a"}},
			true,
		}, {
			"Init_2",
			fields{},
			args{map[string]interface{}{"content": "yaml_config"}},
			true,
		}, {
			"Init_3",
			fields{},
			args{map[string]interface{}{"content": yamlConfig}},
			false,
		}, {
			"Init_4",
			fields{},
			args{map[string]interface{}{"path": FakeConfigPath}},
			false,
		}, {
			"Init_5",
			fields{},
			args{map[string]interface{}{"path": EmptyConfigPath}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			if err := s.Init(tt.args.options); (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_storeOperate_Name(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Name", fields{}, "file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			if got := s.Name(); got != tt.want {
				t.Errorf("Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_storeOperate_Save(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	type args struct {
		key config.PathKey
		val []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Save", fields{}, args{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			if err := s.Save(tt.args.key, tt.args.val); (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_storeOperate_Watch(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	type args struct {
		key config.PathKey
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"Watch",
			fields{make(map[config.PathKey][]chan []byte), make(map[config.PathKey]string)},
			args{"/arana-db/config/data/dataSourceClusters"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			got, err := s.Watch(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Watch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("Watch() got channel is nil")
			}
		})
	}
}

func Test_storeOperate_initCfgJsonMap(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	type args struct {
		val string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"initCfgJsonMap", fields{}, args{jsonConfig}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			s.initCfgJsonMap(tt.args.val)
		})
	}
}

func Test_storeOperate_readFromFile(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	type args struct {
		path string
		cfg  *config.Configuration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"readFromFile_1",
			fields{nil, map[config.PathKey]string{"/arana-db/config/data/dataSourceClusters": "test"}},
			args{FakeConfigPath, &config.Configuration{}},
			false,
		}, {
			"readFromFile_2",
			fields{nil, map[config.PathKey]string{"/arana-db/config/data/dataSourceClusters": "test"}},
			args{"~/testdata/fake_config.yaml", &config.Configuration{}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			if err := s.readFromFile(tt.args.path, tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("readFromFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_storeOperate_searchDefaultConfigFile(t *testing.T) {
	type fields struct {
		receivers map[config.PathKey][]chan []byte
		cfgJson   map[config.PathKey]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
		want1  bool
	}{
		{"searchDefaultConfigFile", fields{}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &storeOperate{
				receivers: tt.fields.receivers,
				cfgJson:   tt.fields.cfgJson,
			}
			got, got1 := s.searchDefaultConfigFile()
			if got != tt.want {
				t.Errorf("searchDefaultConfigFile() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("searchDefaultConfigFile() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
