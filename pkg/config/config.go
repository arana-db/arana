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

package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

import (
	"github.com/tidwall/gjson"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	ConfigKeyMapping map[PathKey]string = map[PathKey]string{
		DefaultConfigMetadataPath:           "metadata",
		DefaultConfigDataTenantsPath:        "data.tenants",
		DefaultConfigDataFiltersPath:        "data.filters",
		DefaultConfigDataListenersPath:      "data.listeners",
		DefaultConfigDataSourceClustersPath: "data.clusters",
		DefaultConfigDataShardingRulePath:   "data.sharding_rule",
	}

	_configValSupplier map[PathKey]func(cfg *Configuration) interface{} = map[PathKey]func(cfg *Configuration) interface{}{
		DefaultConfigMetadataPath: func(cfg *Configuration) interface{} {
			return &cfg.Metadata
		},
		DefaultConfigDataTenantsPath: func(cfg *Configuration) interface{} {
			return &cfg.Data.Tenants
		},
		DefaultConfigDataFiltersPath: func(cfg *Configuration) interface{} {
			return &cfg.Data.Filters
		},
		DefaultConfigDataListenersPath: func(cfg *Configuration) interface{} {
			return &cfg.Data.Listeners
		},
		DefaultConfigDataSourceClustersPath: func(cfg *Configuration) interface{} {
			return &cfg.Data.DataSourceClusters
		},
		DefaultConfigDataShardingRulePath: func(cfg *Configuration) interface{} {
			return &cfg.Data.ShardingRule
		},
	}
)

type Changeable interface {
	Name() string
	Sign() string
}

type Observer func()

type ConfigOptions struct {
	StoreName string                 `yaml:"name"`
	Options   map[string]interface{} `yaml:"options"`
}

type Center struct {
	initialize   int32
	storeOperate StoreOperate
	confHolder   atomic.Value // 里面持有了最新的 *Configuration 对象
	lock         sync.RWMutex
	observers    []Observer
	watchCancels []context.CancelFunc
}

func NewCenter(options ConfigOptions) (*Center, error) {
	if err := Init(options.StoreName, options.Options); err != nil {
		return nil, err
	}

	operate, err := GetStoreOperate()
	if err != nil {
		return nil, err
	}

	return &Center{
		confHolder:   atomic.Value{},
		lock:         sync.RWMutex{},
		storeOperate: operate,
		observers:    make([]Observer, 0, 2),
	}, nil
}

func (c *Center) Close() error {
	if err := c.storeOperate.Close(); err != nil {
		return err
	}

	for i := range c.watchCancels {
		c.watchCancels[i]()
	}

	return nil
}

func (c *Center) Load() (*Configuration, error) {
	return c.LoadContext(context.Background())
}

func (c *Center) LoadContext(ctx context.Context) (*Configuration, error) {
	val := c.confHolder.Load()
	if val == nil {
		cfg, err := c.loadFromStore(ctx)
		if err != nil {
			return nil, err
		}
		c.confHolder.Store(cfg)
	}

	val = c.confHolder.Load()
	return val.(*Configuration), nil
}

func (c *Center) loadFromStore(ctx context.Context) (*Configuration, error) {
	operate := c.storeOperate

	cfg := &Configuration{
		TypeMeta: TypeMeta{},
		Metadata: make(map[string]interface{}),
		Data: &Data{
			Filters:            make([]*Filter, 0),
			Listeners:          make([]*Listener, 0),
			Tenants:            make([]*Tenant, 0),
			DataSourceClusters: make([]*DataSourceCluster, 0),
			ShardingRule:       &ShardingRule{},
		},
	}

	for k := range ConfigKeyMapping {
		val, err := operate.Get(k)
		if err != nil {
			return nil, err
		}

		supplier, ok := _configValSupplier[k]

		if !ok {
			return nil, fmt.Errorf("%s not register val supplier", k)
		}

		if len(val) != 0 {
			if err := json.Unmarshal(val, supplier(cfg)); err != nil {
				return nil, err
			}
		}
	}
	return cfg, nil
}

func (c *Center) watchFromStore() error {
	if !atomic.CompareAndSwapInt32(&c.initialize, 0, 1) {
		return nil
	}

	cancels := make([]context.CancelFunc, 0, len(ConfigKeyMapping))

	for k := range ConfigKeyMapping {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		ch, err := c.storeOperate.Watch(k)
		if err != nil {
			return err
		}
		go c.watchKey(ctx, k, ch)
	}

	return nil
}

func (c *Center) watchKey(ctx context.Context, key PathKey, ch <-chan []byte) {
	consumer := func(ret []byte) {
		defer c.lock.Unlock()
		c.lock.Lock()

		supplier, ok := _configValSupplier[key]
		if !ok {
			log.Errorf("%s not register val supplier", key)
			return
		}

		cfg := c.confHolder.Load().(*Configuration)

		if len(ret) != 0 {
			if err := json.Unmarshal(ret, supplier(cfg)); err != nil {
				log.Errorf("", err)
			}
		}

		c.confHolder.Store(cfg)
	}

	for {
		select {
		case ret := <-ch:
			consumer(ret)
		case <-ctx.Done():
			log.Infof("stop watch : %s", key)
		}
	}
}

func (c *Center) Persist() error {
	return c.PersistContext(context.Background())
}

func (c *Center) PersistContext(ctx context.Context) error {
	val := c.confHolder.Load()
	if val == nil {
		return errors.New("ConfHolder.load is nil")
	}

	conf := val.(*Configuration)

	configJson, err := json.Marshal(conf)
	if err != nil {
		return fmt.Errorf("config json.marshal failed  %v err:", err)
	}

	for k, v := range ConfigKeyMapping {

		if err := c.storeOperate.Save(k, []byte(gjson.GetBytes(configJson, v).String())); err != nil {
			return err
		}
	}
	return nil
}
