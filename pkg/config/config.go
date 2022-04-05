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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"sync"
	"sync/atomic"
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
	storeOperate StoreOperate
	confHolder   atomic.Value // 里面持有了最新的 *Configuration 对象
	lock         *sync.RWMutex
	observers    []Observer
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
		storeOperate: operate,
		lock:         &sync.RWMutex{},
		observers:    make([]Observer, 0, 2),
	}, nil
}

func (c *Center) Close() error {
	return c.storeOperate.Close()
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

	metadataVal, err := operate.Get(DefaultConfigMetadataPath)
	if err != nil {
		return nil, err
	}
	listenersVal, err := operate.Get(DefaultConfigDataListenersPath)
	if err != nil {
		return nil, err
	}
	filtersVal, err := operate.Get(DefaultConfigDataFiltersPath)
	if err != nil {
		return nil, err
	}
	clustersVal, err := operate.Get(DefaultConfigDataSourceClustersPath)
	if err != nil {
		return nil, err
	}
	shardingRuleVal, err := operate.Get(DefaultConfigDataShardingRulePath)
	if err != nil {
		return nil, err
	}
	tenantsVal, err := operate.Get(DefaultConfigDataTenantsPath)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(metadataVal, &cfg.Metadata); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(listenersVal, &cfg.Data.Listeners); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(filtersVal, &cfg.Data.Filters); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(clustersVal, &cfg.Data.DataSourceClusters); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(tenantsVal, &cfg.Data.Tenants); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(shardingRuleVal, cfg.Data.ShardingRule); err != nil {
		return nil, err
	}
	return cfg, nil
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

	if err := c.storeOperate.Save(DefaultConfigMetadataPath, []byte(gjson.GetBytes(configJson, "metadata").String())); err != nil {
		return err
	}

	if err := c.storeOperate.Save(DefaultConfigDataListenersPath, []byte(gjson.GetBytes(configJson, "data.listeners").String())); err != nil {
		return err
	}

	if err := c.storeOperate.Save(DefaultConfigDataFiltersPath, []byte(gjson.GetBytes(configJson, "data.filters").String())); err != nil {
		return err
	}

	if err := c.storeOperate.Save(DefaultConfigDataSourceClustersPath, []byte(gjson.GetBytes(configJson, "data.dataSourceClusters").String())); err != nil {
		return err
	}

	if err := c.storeOperate.Save(DefaultConfigDataTenantsPath, []byte(gjson.GetBytes(configJson, "data.tenants").String())); err != nil {
		return err
	}

	if err = c.storeOperate.Save(DefaultConfigDataShardingRulePath, []byte(gjson.GetBytes(configJson, "data.shardingRule").String())); err != nil {
		return err
	}

	return nil
}
