/*
 *  Licensed to Apache Software Foundation (ASF) under one or more contributor
 *  license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright
 *  ownership. Apache Software Foundation (ASF) licenses this file to you under
 *  the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package file

import (
	"encoding/json"
	"fmt"
	"sync"
)

import (
	"github.com/arana-db/arana/pkg/config"

	"github.com/tidwall/gjson"
)

func init() {
	config.Register(&storeOperate{})
}

type storeOperate struct {
	lock      *sync.RWMutex
	receivers map[string][]chan []byte
	path      string
	cfgJson   map[string]string
}

func (s *storeOperate) Init(options map[string]interface{}) error {
	s.lock = &sync.RWMutex{}
	s.receivers = make(map[string][]chan []byte)

	s.path, _ = options["path"].(string)

	cfg, err := config.LoadV2(s.path)
	if err != nil {
		return err
	}
	configJson, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("config json.marshal failed  %v err:", err)
	}
	s.initCfgJsonMap(string(configJson))
	return nil
}

func (s *storeOperate) initCfgJsonMap(val string) {
	s.cfgJson = make(map[string]string)

	s.cfgJson[config.DefaultConfigMetadataPath] = gjson.Get(val, "metadata").String()
	s.cfgJson[config.DefaultConfigDataTenantsPath] = gjson.Get(val, "data.tenants").String()
	s.cfgJson[config.DefaultConfigDataFiltersPath] = gjson.Get(val, "data.filters").String()
	s.cfgJson[config.DefaultConfigDataListenersPath] = gjson.Get(val, "data.listeners").String()
	s.cfgJson[config.DefaultConfigDataSourceClustersPath] = gjson.Get(val, "data.clusters").String()
	s.cfgJson[config.DefaultConfigDataShardingRulePath] = gjson.Get(val, "data.sharding_rule").String()
}

func (s *storeOperate) Save(key string, val []byte) error {
	return nil
}

func (s *storeOperate) Get(key string) ([]byte, error) {
	val := []byte(s.cfgJson[key])
	return val, nil
}

//Watch TODO change notification through file inotify mechanism
func (s *storeOperate) Watch(key string) (<-chan []byte, error) {
	defer s.lock.Unlock()

	if _, ok := s.receivers[key]; !ok {
		s.receivers[key] = make([]chan []byte, 0, 2)
	}

	rec := make(chan []byte)

	s.receivers[key] = append(s.receivers[key], rec)

	return rec, nil
}

func (s *storeOperate) Name() string {
	return "file"
}

func (s *storeOperate) Close() error {
	return nil
}
