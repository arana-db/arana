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
		IdleTimeout    time.Duration   `yaml:"idle_timeout" json:"-"`            // close backend direct connection after idle_timeout
		IdleTimeoutStr string          `yaml:"-" json:"idle_timeout"`
	}

	ConfigMap struct {
		TypeMeta
		Metadata *ObjectMeta `yaml:"metadata" json:"metadata"`
		Data     *ConfigData `validate:"required" yaml:"data" json:"data"`
	}

	TypeMeta struct {
		Kind       string `yaml:"kind" json:"kind,omitempty"`
		APIVersion string ` yaml:"apiVersion" json:"apiVersion,omitempty"`
	}

	ObjectMeta struct {
		Name string `json:"name,omitempty" yaml:"apiVersion,omitempty"`
	}

	ConfigData struct {
		Listeners          []*ListenerV2        `validate:"required" yaml:"listeners" json:"listeners"`
		Executors          []*ExecutorV2        `validate:"required" yaml:"executors" json:"executors"`
		DataSourceClusters []*DataSourceCluster `validate:"required" yaml:"dataSourceClusters" json:"dataSourceClusters"`
		ShardingRule       *ShardingRule        `yaml:"shardingRule,omitempty" json:"shardingRule,omitempty"`
	}

	DataSourceCluster struct {
		Name        string         `yaml:"name" json:"name"`
		Type        DataSourceType `yaml:"type" json:"type"`
		SqlMaxLimit int            `default:"-1" yaml:"sqlMaxLimit" json:"sqlMaxLimit,omitempty"`
		Tenant      string         `yaml:"tenant" json:"tenant"`
		ConnProps   *ConnProp      `yaml:"connProps" json:"connProps,omitempty"`
		Groups      []*Group       `yaml:"groups" json:"groups"`
	}

	ConnProp struct {
		Capacity    int `yaml:"capacity" json:"capacity,omitempty"`       // connection pool capacity
		MaxCapacity int `yaml:"maxCapacity" json:"maxCapacity,omitempty"` // max connection pool capacity
		IdleTimeout int `yaml:"idleTimeout" json:"idleTimeout,omitempty"` // close backend direct connection after idle_timeout
	}

	Group struct {
		Name    string `yaml:"name" json:"name"`
		AtomDbs []*Dsn `yaml:"atomDbs" json:"atomDbs"`
	}

	Dsn struct {
		Host     string         `yaml:"host" json:"host"`
		Port     int            `yaml:"port" json:"port"`
		Username string         `yaml:"username" json:"username"`
		Password string         `yaml:"password" json:"password"`
		Database string         `yaml:"database" json:"database"`
		DsnProps *DsnConnProp   `yaml:"connProps" json:"connProps,omitempty"`
		Role     DataSourceRole `yaml:"role" json:"role"`
		Weight   string         `default:"r10w10" yaml:"weight" json:"weight"`
	}

	DsnConnProp struct {
		ReadTimeout  string `yaml:"readTimeout" json:"readTimeout,omitempty"`
		WriteTimeout string `yaml:"writeTimeout" json:"writeTimeout,omitempty"`
		ParseTime    bool   `default:"true" yaml:"parseTime,omitempty" json:"parseTime,omitempty"`
		Loc          string `yaml:"loc" json:"loc,omitempty"`
		Charset      string `yaml:"charset" json:"charset,omitempty"`
	}

	ShardingRule struct {
		Tables []*Table `yaml:"tables" json:"tables"`
	}

	ListenerV2 struct {
		ProtocolType  string            `yaml:"protocol_type" json:"protocol_type"`
		SocketAddress *SocketAddress    `yaml:"socket_address" json:"socket_address"`
		Config        *ListenerV2Config `yaml:"config" json:"config"`
		Executor      string            `yaml:"executor" json:"executor"`
	}

	ListenerV2Config struct {
		ServerVersion string  `yaml:"server_version" json:"server_version"`
		Users         []*User `yaml:"users" json:"users"`
	}

	User struct {
		Username string `yaml:"username" json:"username"`
		Password string `yaml:"password" json:"password"`
	}

	ExecutorV2 struct {
		Name        string                   `yaml:"name" json:"name"`
		Mode        string                   `yaml:"mode" json:"mode"`
		DataSources []*ExecutorV2DataSources `yaml:"data_sources" json:"data_sources"`
	}

	ExecutorV2DataSources struct {
		Master string `yaml:"master" json:"master"`
	}

	Table struct {
		Name            string    `yaml:"name" json:"name"`
		AllowFullScan   bool      `yaml:"allowFullScan" json:"allowFullScan,omitempty"`
		ActualDataNodes []string  `yaml:"actualDataNodes" json:"actualDataNodes"`
		DbRules         []*Rule   `yaml:"dbRules" json:"dbRules"`
		TblRules        []*Rule   `yaml:"tblRules" json:"tblRules"`
		TblProps        *TblProps `yaml:"otherProps" json:"otherProps,omitempty"`
		Topology        *Topology `yaml:"topology" json:"topology"`
		ShadowTopology  *Topology `yaml:"shadowTopology" json:"shadowTopology"`
	}

	Rule struct {
		Column string `yaml:"column" json:"column"`
		Expr   string `yaml:"expr" json:"expr"`
	}

	TblProps struct {
		SqlMaxLimit int `default:"-1" yaml:"sqlMaxLimit" json:"sqlMaxLimit"` // connection pool capacity
	}

	Topology struct {
		DbPattern  string `yaml:"dbPattern" json:"dbPattern"`
		TblPattern string `yaml:"tblPattern" json:"tblPattern"`
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
		return errors.Errorf("unrecognized datasource role: %q", text)
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
