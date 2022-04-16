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
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/testdata"
)

var (
	mockConfData = map[config.PathKey]string{
		config.DefaultConfigPath:                   "",
		config.DefaultConfigMetadataPath:           "",
		config.DefaultConfigDataListenersPath:      "",
		config.DefaultConfigDataFiltersPath:        "",
		config.DefaultConfigDataSourceClustersPath: "",
		config.DefaultConfigDataShardingRulePath:   "",
		config.DefaultConfigDataTenantsPath:        "",
	}

	cfg *config.Configuration
)

func doDataMock() {
	cfg, _ = config.LoadV2(testdata.Path("fake_config.yaml"))

	data, _ := json.Marshal(cfg)

	for k, v := range config.ConfigKeyMapping {
		mockConfData[k] = string(gjson.GetBytes(data, v).String())
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
		lock:      sync.RWMutex{},
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

//CancelListenConfig use to cancel listen config change
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
		confMap:    make(map[config.PathKey]string),
		cfgLock:    &sync.RWMutex{},
		lock:       &sync.RWMutex{},
		receivers:  make(map[config.PathKey]*nacosWatcher),
		cancelList: []context.CancelFunc{},
	}

	doDataMock()
	return operate
}

func Test_loadDataFromServer(t *testing.T) {
	operate := buildOperate()
	defer operate.client.CloseClient()

	err := operate.loadDataFromServer()
	assert.NoError(t, err, "")

	for k, v := range operate.confMap {
		assert.Equalf(t, mockConfData[k], v, "%s should be equal", k)
	}
}

func Test_watch(t *testing.T) {
	operate := buildOperate()
	defer operate.client.CloseClient()

	err := operate.loadDataFromServer()
	assert.NoError(t, err, "should be success")

	assert.NoError(t, err, "should be success")

	newCfg, _ := config.LoadV2(testdata.Path("fake_config.yaml"))

	newCfg.Data.Filters = append(newCfg.Data.Filters, &config.Filter{
		Name:   "arana-nacos-watch",
		Config: []byte("{\"arana-nacos-watch\":\"arana-nacos-watch\"}"),
	})

	receiver, err := operate.Watch(config.DefaultConfigDataFiltersPath)
	assert.NoError(t, err, "should be success")

	data, err := json.Marshal(newCfg)
	assert.NoError(t, err, "should be marshal success")

	for k, v := range config.ConfigKeyMapping {
		if k == config.DefaultConfigDataFiltersPath {
			operate.client.PublishConfig(vo.ConfigParam{
				DataId:  string(config.DefaultConfigDataFiltersPath),
				Content: string(gjson.GetBytes(data, v).String()),
			})
		}
	}

	t.Logf("new config val : %s", string(data))

	ret := <-receiver

	expectVal := string(gjson.GetBytes(data, config.ConfigKeyMapping[config.DefaultConfigDataFiltersPath]).String())

	t.Logf("expect val : %s", expectVal)
	t.Logf("acutal val : %s", string(ret))

	assert.Equal(t, expectVal, string(ret))
}
