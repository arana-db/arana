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
	"fmt"
	"time"
)

import (
	"github.com/pkg/errors"
)

type (
	// DataSourceRole is the data source role
	DataSourceRole int

	// DataSourceType is the data source type
	DataSourceType int

	// DataSource is the data source
	DataSource struct {
		Role           DataSourceRole  `yaml:"role" json:"role"`
		Type           DataSourceType  `yaml:"type" json:"type"`
		Name           string          `yaml:"name" json:"name"`
		Conf           json.RawMessage `yaml:"conf" json:"conf"`
		Capacity       int             `yaml:"capacity" json:"capacity"`         // connection pool capacity
		MaxCapacity    int             `yaml:"max_capacity" json:"max_capacity"` // max connection pool capacity
		IdleTimeout    time.Duration   `yaml:"idle_timeout" json:"_"`            // close backend direct connection after idle_timeout
		IdleTimeoutStr string          `yaml:"_" json:"idle_timeout"`
	}

	AranaConfig struct {
		Kind      string     `yaml:"kind" json:"kind"`
		Metadata  Metadata   `yaml:"metadata" json:"metadata"`
		Data      ConfigData `yaml:"config_data" json:"config_data"`
		Listeners []Listener `yaml:"listeners" json:"listeners"`
		Executors []Executor `yaml:"executors" json:"executors"`
	}

	Metadata struct {
		Name string `yaml:"name" json:"name"`
	}

	ConfigData struct {
		ApiVersion         string              `yaml:"api_version" json:"api_version"`
		DataSourceClusters []DataSourceCluster `yaml:"data_source_clusters" json:"data_source_clusters"`
		ShardingRules      []ShardingRule      `yaml:"sharding_rules" json:"sharding_rules"`
	}

	DataSourceCluster struct {
		Name        string         `yaml:"name" json:"name"`
		Type        DataSourceType `yaml:"type" json:"type"`
		SqlMaxLimit int            `yaml:"sql_max_limit" json:"sql_max_limit"`
		Tenant      string         `yaml:"tenant" json:"tenant"`
		ConnProps   ConnProps      `yaml:"conf_props" json:"conf_props"`
		Groups      []Group        `yaml:"groups" json:"groups"`
	}

	ConnProps struct {
		Capacity    int           `yaml:"capacity" json:"capacity"`         // connection pool capacity
		MaxCapacity int           `yaml:"max_capacity" json:"max_capacity"` // max connection pool capacity
		IdleTimeout time.Duration `yaml:"idle_timeout" json:"idle_timeout"` // close backend direct connection after idle_timeout
	}

	Group struct {
		Name    string `yaml:"name" json:"name"`
		AtomDbs []Dsn  `yaml:"atom_dbs" json:"atom_dbs"`
	}

	Dsn struct {
		Host     string         `yaml:"host" json:"host"`
		Port     int            `yaml:"port" json:"port"`
		Username string         `yaml:"username" json:"username"`
		Password string         `yaml:"password" json:"password"`
		Database string         `yaml:"database" json:"database"`
		DsnProps DsnProp        `yaml:"props" json:"props"`
		Role     DataSourceRole `yaml:"role" json:"role"`
		Weight   string         `yaml:"weight" json:"weight"`
	}

	DsnProp struct {
		Timeout      int    `yaml:"timeout" json:"timeout"`
		ReadTimeout  string `yaml:"read_timeout" json:"read_timeout"`
		WriteTimeout string `yaml:"write_timeout" json:"write_timeout"`
		Loc          string `yaml:"loc" json:"loc"`
		Charset      string `yaml:"charset" json:"charset"`
	}

	ShardingRule struct {
		Tables []Table `yaml:"tables" json:"tables"`
	}

	Table struct {
		Name            string   `yaml:"name" json:"name"`
		AllowFullScan   bool     `yaml:"allow_full_scan" json:"allow_full_scan"`
		ActualDataNodes string   `yaml:"actual_data_nodes" json:"actual_actual_nodes"`
		DbRules         []Rule   `yaml:"db_rules" json:"db_rules"`
		TblRules        []Rule   `yaml:"tbl_rules" json:"tbl_rules"`
		TblProps        TblProps `yaml:"other_props" json:"other_props"`
		Topology        Topology `yaml:"topology" json:"topology"`
		ShadowTopology  Topology `yaml:"shadow_topology" json:"shadow_topology"`
	}

	Rule struct {
		Column string `yaml:"column" json:"column"`
		Expr   string `yaml:"expr" json:"expr"`
	}

	TblProps struct {
		SqlMaxLimit int `yaml:"sql_max_limit" json:"sql_max_limit"` // connection pool capacity
	}

	Topology struct {
		DbPattern  string `yaml:"db_pattern" json:"db_pattern"`
		TblPattern string `yaml:"tbl_pattern" json:"tbl_pattern"`
	}
)

const (
	Master DataSourceRole = iota
	Slave
	Meta
)

const (
	DBMysql DataSourceType = iota
	DBPostgreSql
)

func (r *DataSourceRole) UnmarshalText(text []byte) error {
	if r == nil {
		return errors.New("can't unmarshal a nil *DataSourceRole")
	}
	if !r.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized datasource role: %q", text)
	}
	return nil
}

func (r *DataSourceRole) unmarshalText(text []byte) bool {
	dataSourceRole := string(text)
	switch dataSourceRole {
	case "master":
		*r = Master
	case "slave":
		*r = Slave
	case "meta":
		*r = Meta
	default:
		return false
	}
	return true
}

func (t *DataSourceType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *DataSourceType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized datasource type: %q", text)
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
