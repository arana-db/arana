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

package config

import (
	"context"
	"fmt"
	"sync/atomic"
)

import (
	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

type (
	CacheConfigReaderTest = cacheConfigReader
)

type cacheConfigReader struct {
	initialize int32
	reader     *configReader
	watcher    *configWatcher
}

func (c *cacheConfigReader) Close() error {
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			return err
		}
	}

	if c.watcher != nil {
		if err := c.watcher.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (c *cacheConfigReader) LoadAll(ctx context.Context) (*Tenant, error) {
	if atomic.CompareAndSwapInt32(&c.initialize, 0, 1) {
		if _, err := c.reader.LoadAll(ctx); err != nil {
			return nil, err
		}

		if err := c.watcher.watchFromStore(c.subscribeConfigChange); err != nil {
			return nil, err
		}
	}

	return c.reader.compositeConfiguration(c.pathInfo()), nil
}

func (c *cacheConfigReader) Load(ctx context.Context, item ConfigItem) (*Tenant, error) {
	if atomic.CompareAndSwapInt32(&c.initialize, 0, 1) {
		if _, err := c.reader.LoadAll(ctx); err != nil {
			return nil, err
		}

		if err := c.watcher.watchFromStore(c.subscribeConfigChange); err != nil {
			return nil, err
		}
	}

	allKeyMap := c.pathInfo()
	ret := make(map[PathKey]string)

	for i := range allKeyMap {
		if allKeyMap[i] == string(item) {
			ret[i] = allKeyMap[i]
		}
	}

	return c.reader.compositeConfiguration(c.pathInfo()), nil
}

func (c *cacheConfigReader) subscribeConfigChange(key PathKey, ret []byte) {
	supplier, ok := c.reader.pathInfo.ConfigValSupplier[key]
	if !ok {
		log.Errorf("%s not register val supplier", key)
		return
	}
	if len(ret) == 0 {
		log.Errorf("%s receive empty content, ignore", key)
		return
	}

	cur := NewEmptyTenant()
	if err := yaml.Unmarshal(ret, supplier(cur)); err != nil {
		log.Errorf("%s marshal new content : %v", key, err)
		return
	}

	c.reader.holders[key].Store(cur)
}

func (c *cacheConfigReader) pathInfo() map[PathKey]string {
	return c.reader.pathInfo.ConfigKeyMapping
}

type configReader struct {
	tenant string

	storeOperate StoreOperator
	pathInfo     *PathInfo
	holders      map[PathKey]*atomic.Value
}

func (c *configReader) Close() error {
	return nil
}

func (c *configReader) LoadAll(ctx context.Context) (*Tenant, error) {
	if err := c.loadFromStore(ctx, c.pathInfo.ConfigKeyMapping); err != nil {
		return nil, err
	}

	return c.compositeConfiguration(c.pathInfo.ConfigKeyMapping), nil
}

func (c *configReader) Load(ctx context.Context, item ConfigItem) (*Tenant, error) {
	allKeyMap := c.pathInfo.ConfigKeyMapping
	ret := make(map[PathKey]string)

	for i := range allKeyMap {
		if allKeyMap[i] == string(item) {
			ret[i] = allKeyMap[i]
		}
	}

	if err := c.loadFromStore(ctx, ret); err != nil {
		return nil, err
	}

	return c.compositeConfiguration(ret), nil
}

func (c *configReader) compositeConfiguration(loadFilter map[PathKey]string) *Tenant {
	conf := &Tenant{
		Name: c.tenant,
	}

	if _, ok := loadFilter[c.pathInfo.DefaultConfigDataUsersPath]; ok {
		if val := c.holders[c.pathInfo.DefaultConfigDataUsersPath].Load(); val != nil {
			conf.Users = val.(*Tenant).Users
		}
	}

	if _, ok := loadFilter[c.pathInfo.DefaultConfigDataNodesPath]; ok {
		if val := c.holders[c.pathInfo.DefaultConfigDataNodesPath].Load(); val != nil {
			conf.Nodes = val.(*Tenant).Nodes
		}
	}

	if _, ok := loadFilter[c.pathInfo.DefaultConfigDataSourceClustersPath]; ok {
		if val := c.holders[c.pathInfo.DefaultConfigDataSourceClustersPath].Load(); val != nil {
			conf.DataSourceClusters = val.(*Tenant).DataSourceClusters
		}
	}

	if _, ok := loadFilter[c.pathInfo.DefaultConfigDataShardingRulePath]; ok {
		if val := c.holders[c.pathInfo.DefaultConfigDataShardingRulePath].Load(); val != nil {
			conf.ShardingRule = val.(*Tenant).ShardingRule
		}
	}

	if _, ok := loadFilter[c.pathInfo.DefaultConfigDataShadowRulePath]; ok {
		if val := c.holders[c.pathInfo.DefaultConfigDataShadowRulePath].Load(); val != nil {
			conf.ShadowRule = val.(*Tenant).ShadowRule
		}
	}

	if conf.Empty() {
		return nil
	}
	return conf
}

func (c *configReader) loadFromStore(ctx context.Context, keyMap map[PathKey]string) error {
	operate := c.storeOperate

	for k := range keyMap {
		val, err := operate.Get(k)
		if err != nil {
			return err
		}

		holder := c.holders[k]
		supplier, ok := c.pathInfo.ConfigValSupplier[k]
		if !ok {
			return fmt.Errorf("%s not register val supplier", k)
		}

		if len(val) != 0 {
			exp := supplier(holder.Load().(*Tenant))
			if err := yaml.Unmarshal(val, exp); err != nil {
				return err
			}
		}
	}
	return nil
}
