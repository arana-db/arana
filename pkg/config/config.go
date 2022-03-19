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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strconv"
)

import (
	"github.com/ghodss/yaml"

	"github.com/pkg/errors"
)

type (
	// ProtocolType protocol type enum
	ProtocolType int32
)

const (
	Http ProtocolType = iota
	Mysql
)

const (
	_ DataSourceType = iota
	DBMysql
	DBPostgreSql
)

type (
	// DataSourceType is the data source type
	DataSourceType int

	// SocketAddress specify either a logical or physical address and port, which are
	// used to tell server where to bind/listen, connect to upstream and find
	// management servers
	SocketAddress struct {
		Address string `default:"0.0.0.0" yaml:"address" json:"address"`
		Port    int    `default:"8881" yaml:"port" json:"port"`
	}

	Filter struct {
		Name   string          `json:"name,omitempty"`
		Config json.RawMessage `json:"config,omitempty"`
	}

	Configuration struct {
		TypeMeta
		Metadata map[string]interface{} `yaml:"metadata" json:"metadata"`
		Data     *Data                  `validate:"required" yaml:"data" json:"data"`
	}

	TypeMeta struct {
		Kind       string `yaml:"kind" json:"kind,omitempty"`
		APIVersion string `yaml:"apiVersion" json:"apiVersion,omitempty"`
	}

	Data struct {
		Filters            []*Filter            `yaml:"filters" json:"filters,omitempty"`
		Listeners          []*Listener          `validate:"required" yaml:"listeners" json:"listeners"`
		Tenants            []*Tenant            `validate:"required" yaml:"tenants" json:"tenants"`
		DataSourceClusters []*DataSourceCluster `validate:"required" yaml:"clusters" json:"clusters"`
		ShardingRule       *ShardingRule        `yaml:"sharding_rule,omitempty" json:"sharding_rule,omitempty"`
	}

	Tenant struct {
		Name  string  `validate:"required" yaml:"name" json:"name"`
		Users []*User `validate:"required" yaml:"users" json:"users"`
	}

	DataSourceCluster struct {
		Name        string         `yaml:"name" json:"name"`
		Type        DataSourceType `yaml:"type" json:"type"`
		SqlMaxLimit int            `default:"-1" yaml:"sql_max_limit" json:"sql_max_limit,omitempty"`
		Tenant      string         `yaml:"tenant" json:"tenant"`
		ConnProps   *ConnProp      `yaml:"conn_props" json:"conn_props,omitempty"`
		Groups      []*Group       `yaml:"groups" json:"groups"`
	}

	ConnProp struct {
		Capacity    int `yaml:"capacity" json:"capacity,omitempty"`         // connection pool capacity
		MaxCapacity int `yaml:"max_capacity" json:"max_capacity,omitempty"` // max connection pool capacity
		IdleTimeout int `yaml:"idle_timeout" json:"idle_timeout,omitempty"` // close backend direct connection after idle_timeout
	}

	Group struct {
		Name  string  `yaml:"name" json:"name"`
		Nodes []*Node `yaml:"nodes" json:"nodes"`
	}

	Node struct {
		Name      string            `yaml:"name" json:"name"`
		Host      string            `yaml:"host" json:"host"`
		Port      int               `yaml:"port" json:"port"`
		Username  string            `yaml:"username" json:"username"`
		Password  string            `yaml:"password" json:"password"`
		Database  string            `yaml:"database" json:"database"`
		ConnProps map[string]string `yaml:"conn_props" json:"conn_props,omitempty"`
		Weight    string            `default:"r10w10" yaml:"weight" json:"weight"`
		Labels    map[string]string `yaml:"labels" json:"labels,omitempty"`
	}

	ShardingRule struct {
		Tables []*Table `yaml:"tables" json:"tables"`
	}

	Listener struct {
		ProtocolType  string         `yaml:"protocol_type" json:"protocol_type"`
		SocketAddress *SocketAddress `yaml:"socket_address" json:"socket_address"`
		ServerVersion string         `yaml:"server_version" json:"server_version"`
	}

	User struct {
		Username string `yaml:"username" json:"username"`
		Password string `yaml:"password" json:"password"`
	}

	Table struct {
		Name           string            `yaml:"name" json:"name"`
		AllowFullScan  bool              `yaml:"allow_full_scan" json:"allow_full_scan,omitempty"`
		DbRules        []*Rule           `yaml:"db_rules" json:"db_rules"`
		TblRules       []*Rule           `yaml:"tbl_rules" json:"tbl_rules"`
		Topology       *Topology         `yaml:"topology" json:"topology"`
		ShadowTopology *Topology         `yaml:"shadow_topology" json:"shadow_topology"`
		Attributes     map[string]string `yaml:"attributes" json:"attributes"`
	}

	Rule struct {
		Column string `yaml:"column" json:"column"`
		Expr   string `yaml:"expr" json:"expr"`
	}

	Topology struct {
		DbPattern  string `yaml:"db_pattern" json:"db_pattern"`
		TblPattern string `yaml:"tbl_pattern" json:"tbl_pattern"`
	}
)

func ParseV2(path string) (*Configuration, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load config")
	}

	if !yamlFormat(path) {
		return nil, errors.Errorf("invalid config file format: %s", filepath.Ext(path))
	}

	var cfg Configuration
	if err = yaml.Unmarshal(content, &cfg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal config")
	}

	return &cfg, nil
}

// LoadV2 loads the configuration.
func LoadV2(path string) (*Configuration, error) {
	configPath, _ := filepath.Abs(path)
	cfg, err := ParseV2(configPath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return cfg, nil
}

func yamlFormat(path string) bool {
	ext := filepath.Ext(path)
	if ext == ".yaml" || ext == ".yml" {
		return true
	}
	return false
}

func (t *DataSourceType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *DataSourceType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return errors.Errorf("unrecognized datasource type: %q", text)
	}
	return nil
}

func (t *DataSourceType) unmarshalText(text []byte) bool {
	dataSourceType := string(text)
	switch dataSourceType {
	case "mysql":
		*t = DBMysql
	case "postgresql":
		*t = DBPostgreSql
	default:
		return false
	}
	return true
}

var reg = regexp.MustCompile(`^[rR]([0-9]+)[wW]([0-9]+)$`)

func (d *Node) GetReadAndWriteWeight() (int, int, error) {
	items := reg.FindStringSubmatch(d.Weight)
	if len(items) != 3 {
		return 0, 0, errors.New("weight config should be r10w10")
	}
	readWeight, err := strconv.Atoi(items[1])
	if err != nil {
		return 0, 0, err
	}
	writeWeight, err := strconv.Atoi(items[2])
	if err != nil {
		return 0, 0, err
	}

	return readWeight, writeWeight, nil
}

func (d *Node) String() string {
	b, _ := json.Marshal(d)
	return string(b)
}

func (t *ProtocolType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *ProtocolType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return errors.Errorf("unrecognized protocol type: %q", text)
	}
	return nil
}

func (t *ProtocolType) unmarshalText(text []byte) bool {
	protocolType := string(text)
	switch protocolType {
	case "mysql":
		*t = Mysql
	case "http":
		*t = Http
	default:
		return false
	}
	return true
}
