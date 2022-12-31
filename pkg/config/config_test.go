/*
 *  Licensed to Apache Software Foundation (ASF) under one or more contributor
 *  license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright
 *  ownership. Apache Software Foundation (ASF) licenses this file to you under
 *  the Apache License, Version 2.0 (the "License"); you may
 *  not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package config_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/config/mock"
)

func TestNewCenter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStoreOperator := mock.NewMockStoreOperator(ctrl)
	shareCh := make(chan []byte)

	defer func() {
		close(shareCh)
	}()

	mockStoreOperator.EXPECT().Watch(gomock.Any()).AnyTimes().Return(shareCh, nil)
	mockStoreOperator.EXPECT().Close().AnyTimes().Return(nil)

	t.Run("close all", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator)
		assert.NoError(t, err)

		cc := c.(*config.CenterTest)

		assert.Nil(t, cc.Reader, "reader need nil")
		assert.Nil(t, cc.Writer, "writer need nil")
		assert.Nil(t, cc.Watcher, "watcher need nil")
	})

	t.Run("only open reader", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator, config.WithReader(true))
		assert.NoError(t, err)

		cc := c.(*config.CenterTest)

		assert.NotNil(t, cc.Reader, "reader not nil")
		assert.Nil(t, cc.Writer, "writer need nil")
		assert.Nil(t, cc.Watcher, "watcher need nil")
	})

	t.Run("only open writer", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator, config.WithWriter(true))
		assert.NoError(t, err)

		cc := c.(*config.CenterTest)

		assert.Nil(t, cc.Reader, "reader need nil")
		assert.NotNil(t, cc.Writer, "writer not nil")
		assert.Nil(t, cc.Watcher, "watcher need nil")
	})

	t.Run("only open watcher", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator, config.WithWatcher(true))
		assert.NoError(t, err)

		cc := c.(*config.CenterTest)

		assert.Nil(t, cc.Reader, "reader need nil")
		assert.Nil(t, cc.Writer, "writer need nil")
		assert.NotNil(t, cc.Watcher, "watcher not nil")
	})

	t.Run("only open readr and open cacheable", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator,
			config.WithReader(true),
			config.WithCacheable(true),
		)
		assert.NoError(t, err)

		cc := c.(*config.CenterTest)

		assert.NotNil(t, cc.Reader, "reader not nil")
		assert.Nil(t, cc.Writer, "writer need nil")
		assert.Nil(t, cc.Watcher, "watcher need nil")

		_, ok := cc.Reader.(*config.CacheConfigReaderTest)
		assert.True(t, ok)
	})
}

func Test_configWatcher_Subscribe(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pi := config.NewPathInfo("arana")

	mockStoreOperator := mock.NewMockStoreOperator(ctrl)
	ch := make(chan []byte)
	shareCh := make(chan []byte)

	defer func() {
		close(shareCh)
	}()

	mockStoreOperator.EXPECT().Watch(pi.DefaultConfigDataNodesPath).AnyTimes().Return(ch, nil)
	mockStoreOperator.EXPECT().Watch(pi.DefaultConfigDataShadowRulePath).AnyTimes().Return(shareCh, nil)
	mockStoreOperator.EXPECT().Watch(pi.DefaultConfigDataUsersPath).AnyTimes().Return(shareCh, nil)
	mockStoreOperator.EXPECT().Watch(pi.DefaultConfigDataShardingRulePath).AnyTimes().Return(shareCh, nil)
	mockStoreOperator.EXPECT().Watch(pi.DefaultConfigDataSourceClustersPath).AnyTimes().Return(shareCh, nil)
	mockStoreOperator.EXPECT().Watch(pi.DefaultConfigSpecPath).AnyTimes().Return(shareCh, nil)
	mockStoreOperator.EXPECT().Close().AnyTimes().Return(nil)

	t.Run("watch", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator, config.WithWatcher(true))
		assert.NoError(t, err)
		funcHolder := atomic.Value{}

		c.Subscribe(context.Background(), config.EventTypeNodes, func(e config.Event) {
			t.Logf("receive event: %#v", e)
			funcHolder.Load().(func(e config.Event))(e)
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			c.Close()
		}()

		cnt := 5
		basePort := 1000

		receiveCnt := int32(0)
		funcHolder.Store(func(e config.Event) {
			atomic.AddInt32(&receiveCnt, 1)
		})

		go func() {
			defer cancel()
			for i := 0; i < cnt; i++ {
				time.Sleep(time.Millisecond * 100)
				ret := config.NewEmptyTenant()
				ret.Nodes = make(map[string]*config.Node)
				ret.Nodes[fmt.Sprintf("node-%d", i)] = &config.Node{
					Name: fmt.Sprintf("node-%d", i),
					Host: fmt.Sprintf("127.0.0.%d", i),
					Port: basePort + i,
				}

				data, _ := yaml.Marshal(ret.Nodes)

				ch <- data
			}
		}()

		<-ctx.Done()
		time.Sleep(5 * time.Second)

		assert.Equal(t, int32(cnt), atomic.LoadInt32(&receiveCnt))
	})
}

func Test_configWriter_Write(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStoreOperator := mock.NewMockStoreOperator(ctrl)

	mockStoreOperator.EXPECT().Save(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
	mockStoreOperator.EXPECT().Close().AnyTimes().Return(nil)

	t.Run("write", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator, config.WithWriter(true))
		assert.NoError(t, err)

		err = c.Write(context.Background(), config.ConfigItemNodes, &config.Tenant{
			Nodes: map[string]*config.Node{
				"nodes-1": {
					Name: "nodes-1",
					Host: "127.0.0.1",
					Port: 1001,
				},
			},
		})
		assert.NoError(t, err)
	})

	t.Run("import", func(t *testing.T) {
		c, err := config.NewCenter("arana", mockStoreOperator, config.WithWriter(true))
		assert.NoError(t, err)

		err = c.Import(context.Background(), &config.Tenant{
			Nodes: map[string]*config.Node{
				"nodes-1": {
					Name: "nodes-1",
					Host: "127.0.0.1",
					Port: 1001,
				},
			},
		})
		assert.NoError(t, err)
	})
}

func Test_configReader_LoadAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("loadOne", func(t *testing.T) {
		callCnt := int32(0)
		mockStoreOperator := mock.NewMockStoreOperator(ctrl)
		mockStoreOperator.EXPECT().Close().AnyTimes().Return(nil)

		nodes := map[string]*config.Node{
			"nodes-1": {
				Name:       "nodes-1",
				Host:       "127.0.0.1",
				Port:       1001,
				Parameters: make(config.ParametersMap),
				ConnProps:  map[string]interface{}{},
				Labels:     map[string]string{},
			},
		}

		data, _ := yaml.Marshal(nodes)

		mockStoreOperator.EXPECT().Get(gomock.Any()).AnyTimes().Return(data, nil).Do(func(interface{}) {
			atomic.AddInt32(&callCnt, 1)
		})

		c, err := config.NewCenter("arana", mockStoreOperator, config.WithReader(true))
		assert.NoError(t, err)

		tenantInfo, err := c.Load(context.Background(), config.ConfigItemNodes)
		assert.NoError(t, err)
		assert.Equal(t, nodes, tenantInfo.Nodes)

		time.Sleep(time.Second)

		tenantInfo, err = c.Load(context.Background(), config.ConfigItemNodes)
		assert.NoError(t, err)
		assert.Equal(t, nodes, tenantInfo.Nodes)

		assert.Equal(t, int32(2), atomic.LoadInt32(&callCnt))

		err = c.Close()
		assert.NoError(t, err)
	})

	t.Run("loadOne-cacheable", func(t *testing.T) {
		callCnt := int32(0)

		mockStoreOperator := mock.NewMockStoreOperator(ctrl)
		mockStoreOperator.EXPECT().Close().AnyTimes().Return(nil)
		shareCh := make(chan []byte)

		defer func() {
			close(shareCh)
		}()

		mockStoreOperator.EXPECT().Watch(gomock.Any()).AnyTimes().Return(shareCh, nil)

		nodes := map[string]*config.Node{
			"nodes-1": {
				Name:       "nodes-1",
				Host:       "127.0.0.1",
				Port:       1001,
				Parameters: make(config.ParametersMap),
				ConnProps:  map[string]interface{}{},
				Labels:     map[string]string{},
			},
		}

		data, _ := yaml.Marshal(nodes)
		users, _ := yaml.Marshal(config.NewEmptyTenant().Users)
		shadow, _ := yaml.Marshal(config.NewEmptyTenant().ShadowRule)
		sharding, _ := yaml.Marshal(config.NewEmptyTenant().ShardingRule)
		clusters, _ := yaml.Marshal(config.NewEmptyTenant().DataSourceClusters)
		spec, _ := yaml.Marshal(config.NewEmptyTenant().Spec)

		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataNodesPath).
			AnyTimes().
			Return(data, nil).
			Do(func(interface{}) {
				atomic.AddInt32(&callCnt, 1)
			})
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataUsersPath).
			AnyTimes().
			Return(users, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataShadowRulePath).
			AnyTimes().
			Return(shadow, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataShardingRulePath).
			AnyTimes().
			Return(sharding, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataSourceClustersPath).
			AnyTimes().
			Return(clusters, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigSpecPath).
			AnyTimes().
			Return(spec, nil)

		c, err := config.NewCenter("arana", mockStoreOperator, config.WithReader(true), config.WithCacheable(true))
		assert.NoError(t, err)

		tenantInfo, err := c.Load(context.Background(), config.ConfigItemNodes)
		assert.NoError(t, err)
		assert.Equal(t, nodes, tenantInfo.Nodes)

		time.Sleep(time.Second)

		_, err = c.Load(context.Background(), config.ConfigItemNodes)
		assert.NoError(t, err)

		assert.Equal(t, int32(1), atomic.LoadInt32(&callCnt))

		err = c.Close()
		assert.NoError(t, err)
	})

	t.Run("loadAll", func(t *testing.T) {
		mockStoreOperator := mock.NewMockStoreOperator(ctrl)
		mockStoreOperator.EXPECT().Close().AnyTimes().Return(nil)
		shareCh := make(chan []byte)

		defer func() {
			close(shareCh)
		}()

		mockStoreOperator.EXPECT().Watch(gomock.Any()).AnyTimes().Return(shareCh, nil)

		nodes, _ := yaml.Marshal(config.NewEmptyTenant().Nodes)
		users, _ := yaml.Marshal(config.NewEmptyTenant().Users)
		shadow, _ := yaml.Marshal(config.NewEmptyTenant().ShadowRule)
		sharding, _ := yaml.Marshal(config.NewEmptyTenant().ShardingRule)
		clusters, _ := yaml.Marshal(config.NewEmptyTenant().DataSourceClusters)
		spec, _ := yaml.Marshal(config.NewEmptyTenant().Spec)

		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataNodesPath).
			AnyTimes().
			Return(nodes, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataUsersPath).
			AnyTimes().
			Return(users, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataShadowRulePath).
			AnyTimes().
			Return(shadow, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataShardingRulePath).
			AnyTimes().
			Return(sharding, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigDataSourceClustersPath).
			AnyTimes().
			Return(clusters, nil)
		mockStoreOperator.EXPECT().Get(config.NewPathInfo("arana").DefaultConfigSpecPath).
			AnyTimes().
			Return(spec, nil)

		c, err := config.NewCenter("arana", mockStoreOperator, config.WithReader(true), config.WithCacheable(true))
		assert.NoError(t, err)

		_, err = c.LoadAll(context.Background())
		assert.NoError(t, err)

		err = c.Close()
		assert.NoError(t, err)
	})
}
