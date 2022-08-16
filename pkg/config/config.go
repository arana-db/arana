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
	"path/filepath"
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
	tenant     string
	initialize int32

	firstLoad int32

	storeOperate StoreOperate
	pathInfo     *PathInfo
	holders      map[PathKey]*atomic.Value

	lock         sync.RWMutex
	observers    map[EventType][]EventSubscriber
	watchCancels []context.CancelFunc
}

type PathInfo struct {
	DefaultConfigSpecPath               PathKey
	DefaultTenantBaseConfigPath         PathKey
	DefaultConfigDataNodesPath          PathKey
	DefaultConfigDataUsersPath          PathKey
	DefaultConfigDataSourceClustersPath PathKey
	DefaultConfigDataShardingRulePath   PathKey
	DefaultConfigDataShadowRulePath     PathKey

	ConfigKeyMapping   map[PathKey]string
	ConfigEventMapping map[PathKey]EventType
	ConfigValSupplier  map[PathKey]func(cfg *Tenant) interface{}
}

func NewPathInfo(tenant string) *PathInfo {

	p := &PathInfo{}

	p.DefaultTenantBaseConfigPath = PathKey(filepath.Join(string(DefaultRootPath), fmt.Sprintf("tenants/%s", tenant)))
	p.DefaultConfigSpecPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "spec"))
	p.DefaultConfigDataNodesPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "nodes"))
	p.DefaultConfigDataUsersPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "users"))
	p.DefaultConfigDataSourceClustersPath = PathKey(filepath.Join(string(p.DefaultTenantBaseConfigPath), "dataSourceClusters"))
	p.DefaultConfigDataShardingRulePath = PathKey(filepath.Join(string(p.DefaultConfigDataSourceClustersPath), "shardingRule"))
	p.DefaultConfigDataShadowRulePath = PathKey(filepath.Join(string(p.DefaultConfigDataSourceClustersPath), "shadowRule"))

	p.ConfigKeyMapping = map[PathKey]string{
		p.DefaultConfigSpecPath:               "spec",
		p.DefaultConfigDataUsersPath:          "users",
		p.DefaultConfigDataSourceClustersPath: "clusters",
		p.DefaultConfigDataShardingRulePath:   "sharding_rule",
		p.DefaultConfigDataNodesPath:          "nodes",
		p.DefaultConfigDataShadowRulePath:     "shadow_rule",
	}

	p.ConfigEventMapping = map[PathKey]EventType{
		p.DefaultConfigDataUsersPath:          EventTypeUsers,
		p.DefaultConfigDataNodesPath:          EventTypeNodes,
		p.DefaultConfigDataSourceClustersPath: EventTypeClusters,
		p.DefaultConfigDataShardingRulePath:   EventTypeShardingRule,
		p.DefaultConfigDataShadowRulePath:     EventTypeShadowRule,
	}

	p.ConfigValSupplier = map[PathKey]func(cfg *Tenant) interface{}{
		p.DefaultConfigDataUsersPath: func(cfg *Tenant) interface{} {
			return &cfg.Users
		},
		p.DefaultConfigDataSourceClustersPath: func(cfg *Tenant) interface{} {
			return &cfg.DataSourceClusters
		},
		p.DefaultConfigDataNodesPath: func(cfg *Tenant) interface{} {
			return &cfg.Nodes
		},
		p.DefaultConfigDataShardingRulePath: func(cfg *Tenant) interface{} {
			return cfg.ShardingRule
		},
		p.DefaultConfigDataShadowRulePath: func(cfg *Tenant) interface{} {
			return cfg.ShadowRule
		},
	}

	return p
}

func NewCenter(tenant string, op StoreOperate) Center {

	p := NewPathInfo(tenant)

	holders := map[PathKey]*atomic.Value{}
	for k := range p.ConfigKeyMapping {
		holders[k] = &atomic.Value{}
		holders[k].Store(&Tenant{})
	}

	return &center{
		firstLoad:    0,
		pathInfo:     p,
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

	if atomic.CompareAndSwapInt32(&c.firstLoad, 0, 1) {
		return nil
	}

	conf := &Tenant{}

	if val := c.holders[c.pathInfo.DefaultConfigDataUsersPath].Load(); val != nil {
		conf.Users = val.(*Tenant).Users
	}
	if val := c.holders[c.pathInfo.DefaultConfigDataNodesPath].Load(); val != nil {
		conf.Nodes = val.(*Tenant).Nodes
	}
	if val := c.holders[c.pathInfo.DefaultConfigDataSourceClustersPath].Load(); val != nil {
		conf.DataSourceClusters = val.(*Tenant).DataSourceClusters
	}
	if val := c.holders[c.pathInfo.DefaultConfigDataShardingRulePath].Load(); val != nil {
		conf.ShardingRule = val.(*Tenant).ShardingRule
	}
	if val := c.holders[c.pathInfo.DefaultConfigDataShadowRulePath].Load(); val != nil {
		conf.ShadowRule = val.(*Tenant).ShadowRule
	}

	return conf
}

func (c *center) Import(ctx context.Context, cfg *Tenant) error {
	return c.doPersist(ctx, cfg)
}

func (c *center) loadFromStore(ctx context.Context) (*Tenant, error) {
	operate := c.storeOperate

	cfg := &Tenant{
		Spec: Spec{
			Metadata: map[string]interface{}{},
		},
		Users:              make([]*User, 0, 4),
		Nodes:              make(map[string]*Node),
		DataSourceClusters: make([]*DataSourceCluster, 0, 4),
		ShardingRule:       new(ShardingRule),
		ShadowRule:         new(ShadowRule),
	}

	for k := range c.pathInfo.ConfigKeyMapping {
		val, err := operate.Get(k)
		if err != nil {
			return nil, err
		}

		supplier, ok := c.pathInfo.ConfigValSupplier[k]

		if !ok {
			//return nil, fmt.Errorf("%s not register val supplier", k)
			continue
		}

		if len(val) != 0 {
			exp := supplier(cfg)
			if err := yaml.Unmarshal(val, exp); err != nil {
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

	cancels := make([]context.CancelFunc, 0, len(c.pathInfo.ConfigKeyMapping))

	for k := range c.pathInfo.ConfigKeyMapping {
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
		supplier, ok := c.pathInfo.ConfigValSupplier[key]
		if !ok {
			log.Errorf("%s not register val supplier", key)
			return
		}

		cfg := c.holders[key].Load().(*Tenant)

		if len(ret) != 0 {
			if err := yaml.Unmarshal(ret, supplier(cfg)); err != nil {
				log.Errorf("", err)
			}
		}

		c.holders[key].Store(cfg)

		et := c.pathInfo.ConfigEventMapping[key]

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

	for k, v := range c.pathInfo.ConfigKeyMapping {

		ret, err := JSONToYAML(gjson.GetBytes(configJson, v).String())
		if err != nil {
			return err
		}

		if err := c.storeOperate.Save(k, ret); err != nil {
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

func JSONToYAML(j string) ([]byte, error) {
	// Convert the JSON to an object.
	var jsonObj interface{}
	// We are using yaml.Unmarshal here (instead of json.Unmarshal) because the
	// Go JSON library doesn't try to pick the right number type (int, float,
	// etc.) when unmarshalling to interface{}, it just picks float64
	// universally. go-yaml does go through the effort of picking the right
	// number type, so we can preserve number type throughout this process.
	err := yaml.Unmarshal([]byte(j), &jsonObj)
	if err != nil {
		return nil, err
	}

	// Marshal this object into YAML.
	return yaml.Marshal(jsonObj)
}
