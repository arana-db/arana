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
 *
 */

package nacos

import (
	"context"
	"strconv"
	"strings"
	"sync"
)

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

const (
	_defaultGroupName string = "arana"

	_namespaceKey string = "namespace"
	_groupKey     string = "group"
	_username     string = "username"
	_password     string = "password"
	_server       string = "server"
	_contextPath  string = "contextPath"
	_scheme       string = "scheme"
)

func init() {
	config.Register(&storeOperate{})
}

//StoreOperate config storage related plugins
type storeOperate struct {
	groupName  string
	client     config_client.IConfigClient
	confMap    map[string]string
	cfgLock    *sync.RWMutex
	lock       *sync.RWMutex
	receivers  map[string]*nacosWatcher
	cancelList []context.CancelFunc
}

//Init plugin initialization
func (s *storeOperate) Init(options map[string]interface{}) error {
	s.lock = &sync.RWMutex{}
	s.cfgLock = &sync.RWMutex{}
	s.confMap = make(map[string]string)
	s.receivers = make(map[string]*nacosWatcher)

	if err := s.initNacosClient(options); err != nil {
		return err
	}
	if err := s.loadDataFromServer(); err != nil {
		return err
	}
	if err := s.doNacosWatch(); err != nil {
		return err
	}

	return nil
}

func (s *storeOperate) initNacosClient(options map[string]interface{}) error {
	s.groupName = _defaultGroupName
	if val, ok := options[_groupKey]; ok {
		s.groupName = val.(string)
	}

	clientConfig := parseClientConfig(options)
	serverConfigs := parseServerConfig(options)

	// a more graceful way to create config client
	client, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ServerConfigs: serverConfigs,
			ClientConfig:  &clientConfig,
		},
	)

	if err != nil {
		return err
	}
	s.client = client
	return nil
}

func parseServerConfig(options map[string]interface{}) []constant.ServerConfig {
	cfgs := make([]constant.ServerConfig, 0)

	scheme := "http"
	if val, ok := options[_scheme]; ok {
		scheme = val.(string)
	}
	contextPath := "/nacos"
	if val, ok := options[_contextPath]; ok {
		contextPath = val.(string)
	}

	if servers, ok := options[_server]; ok {
		addresses := strings.Split(servers.(string), ",")
		for i := range addresses {
			addr := strings.Split(strings.TrimSpace(addresses[i]), ":")

			ip := addr[0]
			port, _ := strconv.ParseInt(addr[1], 10, 64)

			cfgs = append(cfgs, constant.ServerConfig{
				Scheme:      scheme,
				ContextPath: contextPath,
				IpAddr:      ip,
				Port:        uint64(port),
			})
		}
	}

	return cfgs
}

func parseClientConfig(options map[string]interface{}) constant.ClientConfig {
	cc := constant.ClientConfig{}

	if val, ok := options[_namespaceKey]; ok {
		cc.NamespaceId = val.(string)
	}
	if val, ok := options[_username]; ok {
		cc.Username = val.(string)
	}
	if val, ok := options[_password]; ok {
		cc.Password = val.(string)
	}
	return cc
}

func (s *storeOperate) loadDataFromServer() error {
	defer s.cfgLock.Unlock()
	s.cfgLock.Lock()

	for dataId := range config.ConfigKeyMapping {
		data, err := s.client.GetConfig(vo.ConfigParam{
			DataId: dataId,
			Group:  s.groupName,
		})
		if err != nil {
			return err
		}

		s.confMap[dataId] = data
	}

	return nil
}

func (s *storeOperate) doNacosWatch() error {
	for dataId := range config.ConfigKeyMapping {
		err := s.client.ListenConfig(vo.ConfigParam{
			DataId: dataId,
			Group:  s.groupName,
			OnChange: func(namespace, group, dataId, data string) {
				defer s.cfgLock.Unlock()
				s.cfgLock.Lock()

				s.confMap[dataId] = data

				s.receivers[dataId].ch <- []byte(data)
			},
		})

		if err != nil {
			return err
		}
	}

	return nil
}

//Save save a configuration data
func (s *storeOperate) Save(key string, val []byte) error {
	return nil
}

//Get get a configuration
func (s *storeOperate) Get(key string) ([]byte, error) {
	defer s.cfgLock.RUnlock()
	s.cfgLock.RLock()

	val := []byte(s.confMap[key])
	return val, nil
}

//Watch Monitor changes of the key
func (s *storeOperate) Watch(key string) (<-chan []byte, error) {
	defer s.lock.Unlock()
	s.lock.Lock()
	if _, ok := s.receivers[key]; !ok {
		ch := make(chan []byte, 4)
		w := newWatcher(ch)
		ctx, cancel := context.WithCancel(context.Background())
		go w.run(ctx)
		s.cancelList = append(s.cancelList, cancel)
		s.receivers[key] = w
	}

	w := s.receivers[key]

	defer w.lock.Unlock()
	w.lock.Lock()

	rec := make(chan []byte)
	s.receivers[key].receivers = append(s.receivers[key].receivers, rec)
	return rec, nil
}

//Name plugin name
func (s *storeOperate) Name() string {
	return "nacos"
}

//Close do close storeOperate
func (s *storeOperate) Close() error {
	return nil
}

type nacosWatcher struct {
	lock      *sync.RWMutex
	receivers []chan []byte
	ch        chan []byte
}

func newWatcher(ch chan []byte) *nacosWatcher {
	w := &nacosWatcher{
		lock:      &sync.RWMutex{},
		receivers: make([]chan []byte, 0, 2),
		ch:        ch,
	}
	return w
}

func (w *nacosWatcher) run(ctx context.Context) {
	for {
		select {
		case resp := <-w.ch:
			for p := range w.receivers {
				w.receivers[p] <- resp
			}
		case <-ctx.Done():
			for p := range w.receivers {
				close(w.receivers[p])
			}
			return
		}
	}
}
