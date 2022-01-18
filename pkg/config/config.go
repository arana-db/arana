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
	"io/ioutil"
	"path/filepath"
	"time"
)

import (
	"github.com/ghodss/yaml"

	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/util/log"
)

type Configuration struct {
	Listeners []*Listener `yaml:"listeners" json:"listeners"`

	Executors []*Executor `yaml:"executors" json:"executors"`

	Filters []*Filter `yaml:"filters" json:"filters"`

	DataSources []*DataSource `yaml:"data_source_cluster" json:"data_source_cluster"`
}

type (
	// ProtocolType protocol type enum
	ProtocolType int32

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

	DataSourceGroup struct {
		Master string   `yaml:"master" json:"master"`
		Slaves []string `yaml:"slaves,omitempty" json:"slaves,omitempty"`
	}

	Executor struct {
		Name                          string             `yaml:"name" json:"name"`
		Mode                          proto.ExecuteMode  `yaml:"mode" json:"mode"`
		DataSources                   []*DataSourceGroup `yaml:"data_sources" json:"data_sources"`
		Filters                       []string           `yaml:"filters" json:"filters"`
		ProcessDistributedTransaction bool               `yaml:"process_distributed_transaction,omitempty" json:"process_distributed_transaction,omitempty"`
	}

	Listener struct {
		ProtocolType  ProtocolType    `yaml:"protocol_type" json:"protocol_type"`
		SocketAddress SocketAddress   `yaml:"socket_address" json:"socket_address"`
		Filters       []string        `yaml:"filters" json:"filters"`
		Config        json.RawMessage `yaml:"config" json:"config"`
		Executor      string          `yaml:"executor" json:"executor"`
	}
)

const (
	Http ProtocolType = iota
	Mysql
)

func (t *ProtocolType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *ProtocolType")
	}
	if !t.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized protocol type: %q", text)
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

func parse(path string) *Configuration {
	log.Infof("load config from :  %s", path)
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("[config] [default load] load config failed, error: %v", err)
	}
	cfg := &Configuration{}
	if yamlFormat(path) {
		jsonBytes, err := yaml.YAMLToJSON(content)
		if err != nil {
			log.Fatalf("[config] [default load] translate yaml to json error: %v", err)
		}
		content = jsonBytes
	}
	// translate to lower case
	err = json.Unmarshal(content, cfg)
	if err != nil {
		log.Fatalf("[config] [default load] json unmarshal config failed, error: %v", err)
	}

	for _, ds := range cfg.DataSources {
		if ds.IdleTimeoutStr != "" {
			var err error
			if ds.IdleTimeout, err = time.ParseDuration(ds.IdleTimeoutStr); err != nil {
				log.Errorf("[config] [default load] parse idle timeout failed, set to default %s, data source name: %s, error: %v",
					ds.Name, err)
			}
		}
	}
	return cfg
}

func parseV2(path string) *ConfigMap {
	log.Infof("load config from :  %s", path)
	content, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("[config] [default load] load config failed, error: %v", err)
	}
	cfg := &ConfigMap{}
	if yamlFormat(path) {
		err = yaml.Unmarshal(content, cfg)
	} else {
		err = errors.Errorf("[config] [default load] unsupport config file format")
	}
	if err != nil {
		log.Fatalf("[config] [default load] json unmarshal config failed, error: %v", err)
	}

	return cfg
}

func yamlFormat(path string) bool {
	ext := filepath.Ext(path)
	if ext == ".yaml" || ext == ".yml" {
		return true
	}
	return false
}

// Load config file and parse
// todo been deleted
func Load(path string) *Configuration {
	configPath, _ := filepath.Abs(path)
	cfg := parse(configPath)
	return cfg
}

// todo been renamed function name to Load
func LoadV2(path string) *ConfigMap {
	configPath, _ := filepath.Abs(path)
	cfg := parseV2(configPath)
	return cfg
}
