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

package etcd

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"

	"github.com/tidwall/gjson"

	"go.etcd.io/etcd/server/v3/embed"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/testdata"
)

const _defaultEtcdV3WorkDir = "/tmp/dubbo-go-arana/config"

var (
	_fakeConfigPath = testdata.Path("fake_bootstrap.yaml")

	mockConfData = map[config.PathKey]string{
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

func Test_storeOpertae(t *testing.T) {
	DefaultListenPeerURLs := "http://localhost:2382"
	DefaultListenClientURLs := "http://localhost:2381"
	lpurl, _ := url.Parse(DefaultListenPeerURLs)
	lcurl, _ := url.Parse(DefaultListenClientURLs)
	etcdCfg := embed.NewConfig()
	etcdCfg.LPUrls = []url.URL{*lpurl}
	etcdCfg.LCUrls = []url.URL{*lcurl}
	etcdCfg.Dir = _defaultEtcdV3WorkDir
	e, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		t.Log("Server is ready!")
	}

	defer func() {
		t.Logf("server start to stop...")
		e.Server.Stop() // trigger a shutdown
	}()

	time.Sleep(time.Second)

	operate := &storeOperate{}
	err = operate.Init(map[string]interface{}{
		"endpoints": "localhost:2381",
	})

	assert.NoError(t, err, "init must success")

	doDataMock()
	cfg, _ := config.LoadV2(testdata.Path("fake_config.yaml"))
	data, _ := json.Marshal(cfg)
	for k, v := range config.ConfigKeyMapping {
		err := operate.Save(k, []byte(gjson.GetBytes(data, v).String()))
		assert.NoError(t, err, "save must success")
	}

	for k, v := range mockConfData {
		ret, err := operate.Get(k)
		assert.NoErrorf(t, err, "get %s must success", k)
		assert.EqualValuesf(t, v, string(ret), "must equal")
		t.Logf("%s => %s", k, string(ret))
	}
}
