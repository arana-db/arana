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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/pkg/errors"

	"github.com/tidwall/gjson"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/util/env"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	PluginName                             = "file"
	configFilenameList                     = []string{"config.yaml", "config.yml"}
	sp                 config.StoreOperate = &storeOperate{}
)

func init() {
	config.Register(PluginName, func() config.StoreOperate {
		return sp
	})
}

type storeOperate struct {
	initialize int32
	lock       sync.RWMutex
	receivers  map[config.PathKey][]chan []byte
	cfgJson    map[config.PathKey]string
	cancels    []context.CancelFunc
	mapping    map[string]*config.PathInfo
}

func (s *storeOperate) Init(options map[string]interface{}) error {
	if !atomic.CompareAndSwapInt32(&s.initialize, 0, 1) {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels = append(s.cancels, cancel)

	s.mapping = make(map[string]*config.PathInfo)
	s.receivers = make(map[config.PathKey][]chan []byte)
	var (
		content string
		ok      bool
		cfg     config.Configuration
	)

	if content, ok = options["content"].(string); ok && len(content) > 0 {
		if err := yaml.NewDecoder(strings.NewReader(content)).Decode(&cfg); err != nil {
			return errors.Wrapf(err, "failed to unmarshal config")
		}
	} else {
		var path string
		// 1. path in bootstrap.yaml
		// 2. read ARANA_CONFIG_PATH
		// 3. read default path: ./config.y(a)ml -> ./conf/config.y(a)ml -> ~/.arana/config.y(a)ml -> /etc/arana/config.y(a)ml
		if path, ok = options["path"].(string); !ok {
			if path, ok = os.LookupEnv(constants.EnvConfigPath); !ok {
				path, ok = s.searchDefaultConfigFile()
			}
		}

		if !ok {
			return errors.New("no config file found")
		}

		path, err := formatPath(path)
		if err != nil {
			return err
		}
		if err := s.readFromFile(path, &cfg); err != nil {
			return err
		}

		go s.watchFileChange(ctx, path)
	}

	for i := range cfg.Data.Tenants {
		name := cfg.Data.Tenants[i].Name
		s.mapping[name] = config.NewPathInfo(name)
	}

	s.updateCfgJsonMap(cfg, false)
	return nil
}

func (s *storeOperate) updateCfgJsonMap(cfg config.Configuration, notify bool) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.cfgJson = make(map[config.PathKey]string)

	tenants := make([]string, 0, 4)
	for i := range cfg.Data.Tenants {
		tenants = append(tenants, cfg.Data.Tenants[i].Name)

		tmp, _ := json.Marshal(cfg.Data.Tenants[i])
		ret := string(tmp)
		mapping := s.mapping[cfg.Data.Tenants[i].Name]

		for k, v := range mapping.ConfigKeyMapping {
			val, _ := config.JSONToYAML(gjson.Get(ret, v).String())
			s.cfgJson[k] = string(val)
			if notify {
				for i := range s.receivers[k] {
					s.receivers[k][i] <- val
				}
			}
		}
	}

	ret, _ := yaml.Marshal(tenants)

	s.cfgJson[config.DefaultTenantsPath] = string(ret)

	if env.IsDevelopEnvironment() {
		log.Debugf("[ConfigCenter][File] load config content : %#v", s.cfgJson)
	}
}

func (s *storeOperate) Save(key config.PathKey, val []byte) error {
	return nil
}

func (s *storeOperate) Get(key config.PathKey) ([]byte, error) {
	val := []byte(s.cfgJson[key])
	return val, nil
}

// Watch
func (s *storeOperate) Watch(key config.PathKey) (<-chan []byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.receivers[key]; !ok {
		s.receivers[key] = make([]chan []byte, 0, 2)
	}

	rec := make(chan []byte)

	s.receivers[key] = append(s.receivers[key], rec)

	return rec, nil
}

func (s *storeOperate) Name() string {
	return PluginName
}

func (s *storeOperate) Close() error {

	for i := range s.cancels {
		s.cancels[i]()
	}

	return nil
}

func (s *storeOperate) readFromFile(path string, cfg *config.Configuration) error {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Open(path); err != nil {
		return errors.Wrapf(err, "failed to open arana config file '%s'", path)
	}
	defer func() {
		_ = f.Close()
	}()

	if err = config.NewDecoder(f).Decode(cfg); err != nil {
		return errors.Wrapf(err, "failed to parse arana config file '%s'", path)
	}

	return nil
}

func (s *storeOperate) searchDefaultConfigFile() (string, bool) {
	var p string
	for _, it := range constants.GetConfigSearchPathList() {
		for _, filename := range configFilenameList {
			p = filepath.Join(it, filename)
			if _, err := os.Stat(p); err == nil {
				return p, true
			}
		}
	}
	return "", false
}

func formatPath(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = strings.Replace(path, "~", home, 1)
	}

	path = filepath.Clean(path)

	return path, nil
}

func (s *storeOperate) watchFileChange(ctx context.Context, path string) {

	refreshT := time.NewTicker(30 * time.Second)

	oldStat, err := os.Stat(path)
	if err != nil {
		log.Errorf("[ConfigCenter][File] get file=%s stat fail : %s", path, err.Error())
	}

	for {
		select {
		case <-refreshT.C:
			stat, err := os.Stat(path)
			if err != nil {
				log.Errorf("[ConfigCenter][File] get file=%s stat fail : %s", path, err.Error())
				continue
			}

			if stat.ModTime().Equal(oldStat.ModTime()) {
				continue
			}

			cfg := &config.Configuration{}
			if err := s.readFromFile(path, cfg); err != nil {
				log.Errorf("[ConfigCenter][File] read file=%s and marshal to Configuration fail : %s", path, err.Error())
				return
			}

			log.Errorf("[ConfigCenter][File] watch file=%s change : %+v", path, stat.ModTime())
			s.updateCfgJsonMap(*cfg, true)
		case <-ctx.Done():

		}
	}

}
