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
	"fmt"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
)

import (
	"github.com/tidwall/gjson"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/util/env"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	ConfigKeyMapping = map[PathKey]string{
		DefaultConfigMetadataPath:           "metadata",
		DefaultTenantsPath:                  "tenants",
		DefaultConfigDataUsersPath:          "tenants.users",
		DefaultConfigDataSourceClustersPath: "tenants.clusters",
		DefaultConfigDataShardingRulePath:   "tenants.sharding_rule",
		DefaultConfigDataNodesPath:          "tenants.nodes",
		DefaultConfigDataShadowRulePath:     "tenants.shadow",
	}

	ConfigEventMapping = map[PathKey]EventType{
		DefaultConfigDataUsersPath:          EventTypeUsers,
		DefaultConfigDataNodesPath:          EventTypeNodes,
		DefaultConfigDataSourceClustersPath: EventTypeClusters,
		DefaultConfigDataShardingRulePath:   EventTypeShardingRule,
		DefaultConfigDataShadowRulePath:     EventTypeShadowRule,
	}

	_configValSupplier = map[PathKey]func(cfg *Tenant) interface{}{
		DefaultConfigDataSourceClustersPath: func(cfg *Tenant) interface{} {
			return cfg.DataSourceClusters
		},
		DefaultConfigDataShardingRulePath: func(cfg *Tenant) interface{} {
			return &cfg.ShardingRule
		},
		DefaultConfigDataShardingRulePath: func(cfg *Tenant) interface{} {
			return &cfg.ShardingRule
		},
		DefaultConfigDataShardingRulePath: func(cfg *Tenant) interface{} {
			return &cfg.ShardingRule
		},
	}
)

func NewTenantOperate(op StoreOperate) TenantOperate {
	return &tenantOperate{
		op: op,
	}
}

type tenantOperate struct {
	op   StoreOperate
	lock sync.RWMutex
}

func (tp *tenantOperate) Tenants() ([]string, error) {
	tp.lock.RLock()
	defer tp.lock.RUnlock()

	val, err := tp.op.Get(DefaultTenantsPath)
	if err != nil {
		return nil, err
	}

	tenants := make([]string, 0, 4)

	if err := yaml.Unmarshal(val, &tenants); err != nil {
		return nil, err
	}

	return tenants, nil
}

func (tp *tenantOperate) CreateTenant(name string) error {
	return errors.New("implement me")
}

func (tp *tenantOperate) RemoveTenant(name string) error {
	return errors.New("implement me")
}

type center struct {
	tenant       string
	initialize   int32
	storeOperate StoreOperate
	holders      map[PathKey]*atomic.Value

	lock         sync.RWMutex
	observers    map[EventType][]EventSubscriber
	watchCancels []context.CancelFunc
}

func NewCenter(tenant string, op StoreOperate) Center {
	holders := map[PathKey]*atomic.Value{}
	for k := range ConfigKeyMapping {
		holders[k] = &atomic.Value{}
		holders[k].Store(&Configuration{})
	}

	return &center{
		tenant:       tenant,
		holders:      holders,
		storeOperate: op,
		observers:    make(map[EventType][]EventSubscriber),
	}
}

func (c *center) Close() error {
	if err := c.storeOperate.Close(); err != nil {
		return err
	}

	for i := range c.watchCancels {
		c.watchCancels[i]()
	}

	return nil
}

func (c *center) Load(ctx context.Context) (*Tenant, error) {
	val := c.compositeConfiguration()
	if val != nil {
		return val, nil
	}

	cfg, err := c.loadFromStore(ctx)
	if err != nil {
		return nil, err
	}

	out, _ := yaml.Marshal(cfg)
	if env.IsDevelopEnvironment() {
		log.Debugf("load configuration:\n%s", string(out))
	}
	return c.compositeConfiguration(), nil
}

func (c *center) compositeConfiguration() *Tenant {
	conf := &Tenant{}
	conf.Users = c.holders[DefaultConfigDataUsersPath].Load().(*Tenant).Users
	conf.Nodes = c.holders[DefaultConfigDataNodesPath].Load().(*Tenant).Nodes
	conf.DataSourceClusters = c.holders[DefaultConfigDataSourceClustersPath].Load().(*Tenant).DataSourceClusters
	conf.ShardingRule = c.holders[DefaultConfigDataShardingRulePath].Load().(*Tenant).ShardingRule
	conf.ShadowRule = c.holders[DefaultConfigDataShadowRulePath].Load().(*Tenant).ShadowRule

	return conf
}

func (c *center) Import(ctx context.Context, cfg *Tenant) error {
	return c.doPersist(ctx, cfg)
}

func (c *center) loadFromStore(ctx context.Context) (*Tenant, error) {
	operate := c.storeOperate

	cfg := &Tenant{
		Users:              make([]*User, 0, 4),
		Nodes:              make([]*Node, 0, 4),
		DataSourceClusters: make([]*DataSourceCluster, 0, 4),
		ShardingRule:       new(ShardingRule),
		ShadowRule:         new(ShadowRule),
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

func (c *center) watchFromStore() error {
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

	c.watchCancels = cancels
	return nil
}

func (c *center) watchKey(ctx context.Context, key PathKey, ch <-chan []byte) {
	consumer := func(ret []byte) {
		supplier, ok := _configValSupplier[key]
		if !ok {
			log.Errorf("%s not register val supplier", key)
			return
		}

		cfg := c.holders[key].Load().(*Tenant)

		if len(ret) != 0 {
			if err := json.Unmarshal(ret, supplier(cfg)); err != nil {
				log.Errorf("", err)
			}
		}

		c.holders[key].Store(cfg)

		et := ConfigEventMapping[key]

		notify := func() {
			c.lock.RLock()
			defer c.lock.RUnlock()

			v := c.observers[et]

			for i := range v {
				v[i].OnEvent(nil)
			}
		}
		notify()
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

func (c *center) PersistContext(ctx context.Context) error {
	return c.doPersist(ctx, c.compositeConfiguration())
}

func (c *center) doPersist(ctx context.Context, conf *Tenant) error {

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

//Subscribe
func (c *center) Subscribe(ctx context.Context, s ...EventSubscriber) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for i := range s {
		if _, ok := c.observers[s[i].Type()]; !ok {
			c.observers[s[i].Type()] = make([]EventSubscriber, 0, 4)
		}

		v := c.observers[s[i].Type()]
		v = append(v, s[i])

		c.observers[s[i].Type()] = v
	}
}

//UnSubscribe
func (c *center) UnSubscribe(ctx context.Context, s ...EventSubscriber) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for i := range s {
		if _, ok := c.observers[s[i].Type()]; !ok {
			continue
		}

		v := c.observers[s[i].Type()]

		for p := range v {
			if v[p] == s[i] {
				v = append(v[:p], v[p+1:]...)
				break
			}
		}

		c.observers[s[i].Type()] = v
	}
}

func (c *center) Tenant() string {
	return c.tenant
}
