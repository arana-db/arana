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
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
)

const defaultEtcdV3WorkDir = "/tmp/dubbo-go-arana/config"

type ClientTestSuite struct {
	suite.Suite

	etcdConfig struct {
		name      string
		endpoints []string
		timeout   time.Duration
		heartbeat int
	}

	etcd *embed.Etcd

	client *Client
}

func TestUnmarshalTextForProtocolTypeNil(t *testing.T) {
	var protocolType ProtocolType
	var text = []byte("http")
	err := protocolType.UnmarshalText(text)
	assert.Nil(t, err)
	assert.Equal(t, Http, protocolType)
}

func TestUnmarshalTextForUnrecognizedProtocolType(t *testing.T) {
	var protocolType = Http
	var text = []byte("PostgreSQL")
	err := protocolType.UnmarshalText(text)
	assert.Error(t, err)
}

func TestUnmarshalText(t *testing.T) {
	var protocolType = Http
	var text = []byte("mysql")
	err := protocolType.UnmarshalText(text)
	assert.Nil(t, err)
	assert.Equal(t, Mysql, protocolType)
}

func TestIsSlave(t *testing.T) {
	Read := "r10w0"
	assert.Equal(t, true, IsSlave(Read))
	Write := "r0w10"
	assert.Equal(t, false, IsSlave(Write))
	ReadAndWrite := "r10w10"
	assert.Equal(t, false, IsSlave(ReadAndWrite))
}

func TestLoad(t *testing.T) {
	cfg := Load("../../docker/conf/config.yaml")
	assert.Equal(t, Mysql, cfg.Listeners[0].ProtocolType)
	assert.Equal(t, "redirect", cfg.Executors[0].Name)
	assert.Equal(t, 3, len(cfg.DataSources))
	assert.Equal(t, "employees", cfg.DataSources[0].Name)
	assert.Equal(t, Master, cfg.DataSources[0].Role)
	assert.Equal(t, DBMysql, cfg.DataSources[0].Type)
}

// start etcd server
func (suite *ClientTestSuite) SetupSuite() {
	t := suite.T()
	DefaultListenPeerURLs := "http://localhost:2382"
	DefaultListenClientURLs := "http://localhost:2381"
	lpurl, _ := url.Parse(DefaultListenPeerURLs)
	lcurl, _ := url.Parse(DefaultListenClientURLs)
	cfg := embed.NewConfig()
	cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}
	cfg.Dir = defaultEtcdV3WorkDir
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		t.Log("Server is ready!")
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		t.Logf("Server took too long to start!")
	}

	suite.etcd = e
	return
}

// stop etcd server
func (suite *ClientTestSuite) TearDownSuite() {
	suite.etcd.Close()
	if err := os.RemoveAll(defaultConfigPath); err != nil {
		suite.FailNow(err.Error())
	}
}

func (suite *ClientTestSuite) setUpClient() *Client {
	c, err := NewClient(suite.etcdConfig.endpoints)
	if err != nil {
		suite.T().Fatal(err)
	}
	return c
}

func (suite *ClientTestSuite) SetupTest() {
	c := suite.setUpClient()
	suite.client = c
	return
}

func (suite *ClientTestSuite) TestLoadConfigFromEtcd() {
	t := suite.T()
	c := suite.client
	defer suite.client.client.Close()

	if err := c.PutConfigToEtcd(configPath); err != nil {
		t.Fatal(err)
	}

	resp, err := c.LoadConfigFromEtcd(defaultConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	config := LoadV2(configPath)
	configJson, _ := json.Marshal(config)

	if resp != string(configJson) {
		t.Fatalf("expect %s but get %s", string(configJson), resp)
	}
}

func (suite *ClientTestSuite) TestUpdateConfigToEtcd() {
	t := suite.T()
	c := suite.client
	defer suite.client.client.Close()

	resp, err := c.LoadConfigFromEtcd(defaultConfigDataExecutorsPath)
	if err != nil {
		t.Fatal(err)
	}
	jsonSlice := make([]map[string]interface{}, 1)

	json.Unmarshal([]byte(resp), &jsonSlice)

	jsonSlice[0]["name"] = "test"

	configJson, err := json.Marshal(jsonSlice)
	if err != nil {
		t.Fatal(err)
	}

	err = c.UpdateConfigToEtcd(defaultConfigDataExecutorsPath, string(configJson))
	if err != nil {
		t.Fatal(err)
	}

	resp, err = c.LoadConfigFromEtcd(defaultConfigDataExecutorsPath)
	if err != nil {
		t.Fatal(err)
	}
	if resp != string(configJson) {
		t.Fatalf("expect %s but get %s", configJson, resp)
	}
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, &ClientTestSuite{
		etcdConfig: struct {
			name      string
			endpoints []string
			timeout   time.Duration
			heartbeat int
		}{
			name:      "test",
			endpoints: []string{"localhost:2381"},
			timeout:   time.Second,
			heartbeat: 1,
		},
	})
}
