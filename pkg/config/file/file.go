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
	"encoding/json"
	"fmt"
	"sync"
)

import (
	"github.com/pkg/errors"

	"github.com/tidwall/gjson"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

func init() {
	config.Register(&storeOperate{})
}

type storeOperate struct {
	lock      *sync.RWMutex
	receivers map[config.PathKey][]chan []byte
	content   string
	cfgJson   map[config.PathKey]string
}

func (s *storeOperate) Init(options map[string]interface{}) error {
	s.lock = &sync.RWMutex{}
	s.receivers = make(map[config.PathKey][]chan []byte)

	s.content, _ = options["content"].(string)

	var cfg config.Configuration
	if err := yaml.Unmarshal([]byte(s.content), &cfg); err != nil {
		return errors.Wrapf(err, "failed to unmarshal config")
	}
	configJson, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("config json.marshal failed  %v err:", err)
	}
	s.initCfgJsonMap(string(configJson))
	return nil
}

func (s *storeOperate) initCfgJsonMap(val string) {
	s.cfgJson = make(map[config.PathKey]string)

	for k, v := range config.ConfigKeyMapping {
		s.cfgJson[k] = gjson.Get(val, v).String()
	}
}

func (s *storeOperate) Save(key config.PathKey, val []byte) error {
	return nil
}

func (s *storeOperate) Get(key config.PathKey) ([]byte, error) {
	val := []byte(s.cfgJson[key])
	return val, nil
}

//Watch TODO change notification through file inotify mechanism
func (s *storeOperate) Watch(key config.PathKey) (<-chan []byte, error) {
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
