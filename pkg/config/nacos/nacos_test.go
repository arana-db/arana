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
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

import (
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/stretchr/testify/assert"

	"github.com/tidwall/gjson"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/testdata"
)

var (
	mockConfData = map[config.PathKey]string{}
	cfg          *config.Configuration
	mockPath     = map[string]*config.PathInfo{}
)

func doDataMock() {
	cfg, _ = config.Load(testdata.Path("fake_config.yaml"))

	for i := range cfg.Data.Tenants {
		tenant := cfg.Data.Tenants[i]
		mockPath[tenant.Name] = config.NewPathInfo(tenant.Name)

		data, _ := json.Marshal(tenant)

		for k, v := range mockPath[tenant.Name].ConfigKeyMapping {
			mockConfData[k] = gjson.GetBytes(data, v).String()
		}
	}
}

type TestNacosClient struct {
	lock      sync.RWMutex
	listeners map[string][]vo.Listener
	ch        chan vo.ConfigParam
	wait      sync.WaitGroup
}

func newNacosClient() *TestNacosClient {
	client := &TestNacosClient{
		listeners: make(map[string][]vo.Listener),
		ch:        make(chan vo.ConfigParam, 16),
		wait:      sync.WaitGroup{},
	}

	go client.doLongPoll()

	return client
}

func (client *TestNacosClient) doLongPoll() {
	client.wait.Add(1)
	defer client.wait.Done()

	for param := range client.ch {
		func() {
			defer client.lock.RUnlock()
			client.lock.RLock()
			listeners, ok := client.listeners[param.DataId]
			if ok {
				for i := range listeners {
					listeners[i]("", param.Group, param.DataId, param.Content)
				}
			}
		}()
	}
}

// GetConfig use to get config from nacos server
// dataId  require
// group   require
// tenant ==>nacos.namespace optional
func (client *TestNacosClient) GetConfig(param vo.ConfigParam) (string, error) {
	return mockConfData[config.PathKey(param.DataId)], nil
}

// PublishConfig use to publish config to nacos server
// dataId  require
// group   require
// content require
// tenant ==>nacos.namespace optional
func (client *TestNacosClient) PublishConfig(param vo.ConfigParam) (bool, error) {
	mockConfData[config.PathKey(param.DataId)] = param.Content

	client.ch <- param
	return true, nil
}

// DeleteConfig use to delete config
// dataId  require
// group   require
// tenant ==>nacos.namespace optional
func (client *TestNacosClient) DeleteConfig(param vo.ConfigParam) (bool, error) {
	return false, errors.New("implement me")
}

// ListenConfig use to listen config change,it will callback OnChange() when config change
// dataId  require
// group   require
// onchange require
// tenant ==>nacos.namespace optional
func (client *TestNacosClient) ListenConfig(params vo.ConfigParam) (err error) {
	defer client.lock.Unlock()
	client.lock.Lock()

	key := params.DataId

	if _, ok := client.listeners[key]; !ok {
		client.listeners[key] = make([]vo.Listener, 0)
	}

	client.listeners[key] = append(client.listeners[key], params.OnChange)

	return nil
}

// CancelListenConfig use to cancel listen config change
// dataId  require
// group   require
// tenant ==>nacos.namespace optional
func (client *TestNacosClient) CancelListenConfig(params vo.ConfigParam) (err error) {
	return nil
}

// SearchConfig use to search nacos config
// search  require search=accurate--精确搜索  search=blur--模糊搜索
// group   option
// dataId  option
// tenant ==>nacos.namespace optional
// pageNo  option,default is 1
// pageSize option,default is 10
func (client *TestNacosClient) SearchConfig(param vo.SearchConfigParm) (*model.ConfigPage, error) {
	return nil, nil
}

// CloseClient Close the GRPC client
func (client *TestNacosClient) CloseClient() {
	close(client.ch)
	client.wait.Wait()
}

func buildOperate() *storeOperate {
	operate := &storeOperate{
		groupName:  "arana",
		client:     newNacosClient(),
		receivers:  make(map[config.PathKey]*nacosWatcher),
		cancelList: []context.CancelFunc{},
	}

	doDataMock()
	return operate
}

func Test_watch(t *testing.T) {
	operate := buildOperate()
	defer operate.client.CloseClient()

	newCfg, _ := config.Load(testdata.Path("fake_config.yaml"))

	newCfg.Data.Tenants[0].Nodes = map[string]*config.Node{
		"node0": {
			Name:     "node0",
			Host:     "127.0.0.1",
			Port:     3306,
			Username: "arana",
			Password: "arana",
			Database: "mock_db",
		},
	}

	receiver, err := operate.Watch(mockPath[newCfg.Data.Tenants[0].Name].DefaultConfigDataNodesPath)
	assert.NoError(t, err, "should be success")

	data, err := yaml.Marshal(newCfg.Data.Tenants[0].Nodes)
	assert.NoError(t, err, "should be marshal success")

	ok, err := operate.client.PublishConfig(vo.ConfigParam{
		DataId:  buildNacosDataId(string(mockPath[newCfg.Data.Tenants[0].Name].DefaultConfigDataNodesPath)),
		Content: string(data),
	})
	assert.True(t, ok)
	assert.NoError(t, err)

	t.Logf("new config val : %s", string(data))

	ret := <-receiver

	t.Logf("expect val : %s", string(data))
	t.Logf("acutal val : %s", string(ret))

	assert.Equal(t, string(data), string(ret))
}

func Test_storeOpertae(t *testing.T) {
	operate := buildOperate()
	defer operate.client.CloseClient()

	newCfg, _ := config.Load(testdata.Path("fake_config.yaml"))

	err := operate.Save(mockPath[newCfg.Data.Tenants[0].Name].DefaultConfigDataShadowRulePath, []byte(""))

	assert.NoError(t, err, "empty string should be success")

	err = operate.Save(mockPath[newCfg.Data.Tenants[0].Name].DefaultConfigDataShadowRulePath, []byte(" "))

	assert.NoError(t, err, "blank string should be success")

}
