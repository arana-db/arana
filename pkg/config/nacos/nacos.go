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

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/util/bytesconv"
	u_conf "github.com/arana-db/arana/pkg/util/config"
)

var (
	PluginName = "nacos"

	ErrorPublishConfigFail = errors.New("save config into nacos fail")
)

func init() {
	config.Register(&storeOperate{})
}

// StoreOperate config storage related plugins
type storeOperate struct {
	groupName  string
	client     config_client.IConfigClient
	cfgLock    sync.RWMutex
	lock       sync.RWMutex
	receivers  map[config.PathKey]*nacosWatcher
	cancelList []context.CancelFunc
}

// Init plugin initialization
func (s *storeOperate) Init(options map[string]interface{}) error {
	s.receivers = make(map[config.PathKey]*nacosWatcher)

	if err := s.initNacosClient(options); err != nil {
		return err
	}
	return nil
}

func (s *storeOperate) initNacosClient(options map[string]interface{}) error {
	s.groupName = u_conf.DefaultGroupName
	if val, ok := options[u_conf.GroupKey]; ok {
		s.groupName = val.(string)
	}

	clientConfig := u_conf.ParseNacosClientConfig(options)
	serverConfigs := u_conf.ParseNacosServerConfig(options)

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
	if val, ok := options[u_conf.Scheme]; ok {
		scheme = val.(string)
	}
	contextPath := "/nacos"
	if val, ok := options[u_conf.ContextPath]; ok {
		contextPath = val.(string)
	}

	if servers, ok := options[u_conf.Server]; ok {
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

	if val, ok := options[u_conf.NamespaceIdKey]; ok {
		cc.NamespaceId = val.(string)
	}
	if val, ok := options[u_conf.Username]; ok {
		cc.Username = val.(string)
	}
	if val, ok := options[u_conf.Password]; ok {
		cc.Password = val.(string)
	}
	return cc
}

// Save save a configuration data
func (s *storeOperate) Save(key config.PathKey, val []byte) error {
	content := bytesconv.BytesToString(val)
	if strings.TrimSpace(content) == "" {
		content = "null"
	}
	ok, err := s.client.PublishConfig(vo.ConfigParam{
		Group:   s.groupName,
		DataId:  buildNacosDataId(string(key)),
		Content: content,
	})
	if err != nil {
		return err
	}

	if !ok {
		return ErrorPublishConfigFail
	}
	return nil
}

// Get get a configuration
func (s *storeOperate) Get(key config.PathKey) ([]byte, error) {
	ret, err := s.client.GetConfig(vo.ConfigParam{
		DataId: buildNacosDataId(string(key)),
		Group:  s.groupName,
	})
	if err != nil {
		return nil, err
	}

	return []byte(ret), nil
}

// Watch Monitor changes of the key
func (s *storeOperate) Watch(key config.PathKey) (<-chan []byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.receivers[key]; !ok {
		w, err := s.newWatcher(key, s.client)
		if err != nil {
			return nil, err
		}

		ctx, cancel := context.WithCancel(context.Background())
		go w.run(ctx)
		s.cancelList = append(s.cancelList, cancel)
		s.receivers[key] = w
	}

	w := s.receivers[key]

	w.lock.Lock()
	defer w.lock.Unlock()

	rec := make(chan []byte)
	s.receivers[key].receivers = append(s.receivers[key].receivers, rec)
	return rec, nil
}

// Name plugin name
func (s *storeOperate) Name() string {
	return PluginName
}

// Close closes storeOperate
func (s *storeOperate) Close() error {
	s.client.CloseClient()
	return nil
}

type nacosWatcher struct {
	lock      sync.RWMutex
	receivers []chan []byte
	ch        chan []byte
}

func (s *storeOperate) newWatcher(key config.PathKey, client config_client.IConfigClient) (*nacosWatcher, error) {
	w := &nacosWatcher{
		receivers: make([]chan []byte, 0, 2),
		ch:        make(chan []byte, 4),
	}

	err := client.ListenConfig(vo.ConfigParam{
		DataId: buildNacosDataId(string(key)),
		Group:  s.groupName,
		OnChange: func(_, _, dataId, content string) {
			s.cfgLock.Lock()
			defer s.cfgLock.Unlock()

			dataId = revertNacosDataId(dataId)
			s.receivers[config.PathKey(dataId)].ch <- []byte(content)
		},
	})
	if err != nil {
		return nil, err
	}

	return w, nil
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

func buildNacosDataId(v string) string {
	return strings.ReplaceAll(v, "/", u_conf.PathSplit)
}

func revertNacosDataId(v string) string {
	return strings.ReplaceAll(v, u_conf.PathSplit, "/")
}
