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
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"

	"github.com/tidwall/gjson"

	"go.etcd.io/etcd/server/v3/embed"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/testdata"
)

const _defaultEtcdV3WorkDir = "/tmp/arana/config"

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
	cfg, _ := config.Load(testdata.Path("fake_config.yaml"))

	tenantName := cfg.Data.Tenants[0].Name

	for k, v := range mockConfData {
		err := operate.Save(k, []byte(v))
		assert.NoError(t, err, "save must success")
	}

	for k, v := range mockConfData {
		ret, err := operate.Get(k)
		assert.NoErrorf(t, err, "get %s must success", k)
		assert.EqualValuesf(t, v, string(ret), "must equal")
		t.Logf("%s => %s", k, string(ret))
	}

	receiver, err := operate.Watch(mockPath[tenantName].DefaultConfigDataUsersPath)
	assert.NoError(t, err, "watch must success")

	newCfg, _ := config.Load(testdata.Path("fake_config.yaml"))
	newCfg.Data.Tenants[0].Users = []*config.User{
		{
			Username: "arana",
			Password: "arana",
		},
	}
	data, _ := yaml.Marshal(newCfg.Data.Tenants[0].Users)

	_, err = operate.client.Put(context.TODO(), string(mockPath[tenantName].DefaultConfigDataUsersPath), string(data))
	assert.NoError(t, err, "put to etcd must success")

	ret := <-receiver

	t.Logf("expect val : %s", string(data))
	t.Logf("acutal val : %s", string(ret))

	assert.Equal(t, string(data), string(ret))
}
