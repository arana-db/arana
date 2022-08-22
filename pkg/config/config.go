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
	"github.com/arana-db/arana/pkg/util/log"
)

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
	BuildEventMapping  map[EventType]func(pre, cur *Tenant) Event
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

	p.ConfigEventMapping = map[PathKey]EventType{
		p.DefaultConfigDataUsersPath:          EventTypeUsers,
		p.DefaultConfigDataNodesPath:          EventTypeNodes,
		p.DefaultConfigDataSourceClustersPath: EventTypeClusters,
		p.DefaultConfigDataShardingRulePath:   EventTypeShardingRule,
		p.DefaultConfigDataShadowRulePath:     EventTypeShadowRule,
	}

	p.ConfigValSupplier = map[PathKey]func(cfg *Tenant) interface{}{
		p.DefaultConfigSpecPath: func(cfg *Tenant) interface{} {
			return &cfg.Spec
		},
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

	p.ConfigKeyMapping = map[PathKey]string{
		p.DefaultConfigSpecPath:               "spec",
		p.DefaultConfigDataUsersPath:          "users",
		p.DefaultConfigDataSourceClustersPath: "clusters",
		p.DefaultConfigDataShardingRulePath:   "sharding_rule",
		p.DefaultConfigDataNodesPath:          "nodes",
		p.DefaultConfigDataShadowRulePath:     "shadow_rule",
	}

	p.BuildEventMapping = map[EventType]func(pre *Tenant, cur *Tenant) Event{
		EventTypeNodes: func(pre, cur *Tenant) Event {
			return Nodes(cur.Nodes).Diff(pre.Nodes)
		},
		EventTypeUsers: func(pre, cur *Tenant) Event {
			return Users(cur.Users).Diff(pre.Users)
		},
		EventTypeClusters: func(pre, cur *Tenant) Event {
			return Clusters(cur.DataSourceClusters).Diff(pre.DataSourceClusters)
		},
		EventTypeShardingRule: func(pre, cur *Tenant) Event {
			return cur.ShardingRule.Diff(pre.ShardingRule)
		},
		EventTypeShadowRule: func(pre, cur *Tenant) Event {
			return cur.ShadowRule.Diff(pre.ShadowRule)
		},
	}

	return p
}

func NewTenantOperate(op StoreOperate) (TenantOperate, error) {
	tenantOp := &tenantOperate{
		op:        op,
		tenants:   map[string]struct{}{},
		cancels:   []context.CancelFunc{},
		observers: &observerBucket{observers: map[EventType][]*subscriber{}},
	}

	if err := tenantOp.init(); err != nil {
		return nil, err
	}

	return tenantOp, nil
}

type tenantOperate struct {
	op   StoreOperate
	lock sync.RWMutex

	tenants   map[string]struct{}
	observers *observerBucket

	cancels []context.CancelFunc
}

func (tp *tenantOperate) Subscribe(ctx context.Context, c callback) context.CancelFunc {
	return tp.observers.add(EventTypeTenants, c)
}

func (tp *tenantOperate) init() error {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	if len(tp.tenants) == 0 {
		val, err := tp.op.Get(DefaultTenantsPath)
		if err != nil {
			return err
		}

		tenants := make([]string, 0, 4)
		if err := yaml.Unmarshal(val, &tenants); err != nil {
			return err
		}

		for i := range tenants {
			tp.tenants[tenants[i]] = struct{}{}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	tp.cancels = append(tp.cancels, cancel)

	return tp.watchTenants(ctx)
}

func (tp *tenantOperate) watchTenants(ctx context.Context) error {
	ch, err := tp.op.Watch(DefaultTenantsPath)
	if err != nil {
		return err
	}

	go func(ctx context.Context) {
		consumer := func(ret []byte) {
			tenants := make([]string, 0, 4)
			if err := yaml.Unmarshal(ret, &tenants); err != nil {
				log.Errorf("marshal tenants content : %v", err)
				return
			}

			event := Tenants(tenants).Diff(tp.ListTenants())
			log.Infof("receive tenants change event : %#v", event)
			tp.observers.notify(EventTypeTenants, event)

			tp.lock.Lock()
			defer tp.lock.Unlock()

			tp.tenants = map[string]struct{}{}
			for i := range tenants {
				tp.tenants[tenants[i]] = struct{}{}
			}
		}

		for {
			select {
			case ret := <-ch:
				consumer(ret)
			case <-ctx.Done():
				log.Infof("stop watch : %s", DefaultTenantsPath)
			}
		}
	}(ctx)

	return nil
}

func (tp *tenantOperate) ListTenants() []string {
	tp.lock.RLock()
	defer tp.lock.RUnlock()

	ret := make([]string, 0, len(tp.tenants))

	for i := range tp.tenants {
		ret = append(ret, i)
	}

	return ret
}

func (tp *tenantOperate) CreateTenant(name string) error {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	if _, ok := tp.tenants[name]; ok {
		return nil
	}

	tp.tenants[name] = struct{}{}
	ret := make([]string, 0, len(tp.tenants))
	for i := range tp.tenants {
		ret = append(ret, i)
	}

	data, err := yaml.Marshal(ret)
	if err != nil {
		return err
	}

	if err := tp.op.Save(DefaultTenantsPath, data); err != nil {
		return errors.Wrap(err, "create tenant name")
	}

	//need to insert the relevant configuration data under the relevant tenant
	tenantPathInfo := NewPathInfo(name)
	for i := range tenantPathInfo.ConfigKeyMapping {
		if err := tp.op.Save(i, []byte("")); err != nil {
			return errors.Wrap(err, fmt.Sprintf("create tenant resource : %s", i))
		}
	}

	return nil
}

func (tp *tenantOperate) RemoveTenant(name string) error {
	tp.lock.Lock()
	defer tp.lock.Unlock()

	delete(tp.tenants, name)

	ret := make([]string, 0, len(tp.tenants))
	for i := range tp.tenants {
		ret = append(ret, i)
	}

	data, err := yaml.Marshal(ret)
	if err != nil {
		return err
	}

	return tp.op.Save(DefaultTenantsPath, data)
}

func (tp *tenantOperate) Close() error {
	for i := range tp.cancels {
		tp.cancels[i]()
	}
	return nil
}

type observerBucket struct {
	lock      sync.RWMutex
	observers map[EventType][]*subscriber
}

func (b *observerBucket) notify(et EventType, val Event) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	v := b.observers[et]
	for i := range v {
		item := v[i]
		select {
		case <-item.ctx.Done():
		default:
			item.watch(val)
		}
	}
}

func (b *observerBucket) add(et EventType, f callback) context.CancelFunc {
	b.lock.Lock()
	defer b.lock.Unlock()

	if _, ok := b.observers[et]; !ok {
		b.observers[et] = make([]*subscriber, 0, 4)
	}

	ctx, cancel := context.WithCancel(context.Background())

	v := b.observers[et]
	v = append(v, &subscriber{
		watch: f,
		ctx:   ctx,
	})

	b.observers[et] = v

	return cancel
}

type center struct {
	tenant     string
	initialize int32

	storeOperate StoreOperate
	pathInfo     *PathInfo
	holders      map[PathKey]*atomic.Value

	observers    *observerBucket
	watchCancels []context.CancelFunc
}

func NewCenter(tenant string, op StoreOperate) Center {

	p := NewPathInfo(tenant)

	holders := map[PathKey]*atomic.Value{}
	for k := range p.ConfigKeyMapping {
		holders[k] = &atomic.Value{}
		holders[k].Store(NewEmptyTenant())
	}

	return &center{
		pathInfo:     p,
		tenant:       tenant,
		holders:      holders,
		storeOperate: op,
		observers:    &observerBucket{observers: map[EventType][]*subscriber{}},
	}
}

func (c *center) Close() error {
	for i := range c.watchCancels {
		c.watchCancels[i]()
	}
	return nil
}

func (c *center) Load(ctx context.Context) (*Tenant, error) {
	if atomic.CompareAndSwapInt32(&c.initialize, 0, 1) {
		if err := c.loadFromStore(ctx); err != nil {
			return nil, err
		}

		if err := c.watchFromStore(); err != nil {
			return nil, err
		}
	}

	return c.compositeConfiguration(), nil
}

func (c *center) compositeConfiguration() *Tenant {
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

	if conf.Empty() {
		return nil
	}
	return conf
}

func (c *center) Import(ctx context.Context, cfg *Tenant) error {
	return c.doPersist(ctx, cfg)
}

func (c *center) loadFromStore(ctx context.Context) error {
	operate := c.storeOperate

	for k := range c.pathInfo.ConfigKeyMapping {
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

func (c *center) watchFromStore() error {
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
		if len(ret) == 0 {
			log.Errorf("%s receive empty content, ignore", key)
			return
		}

		cur := NewEmptyTenant()
		if err := yaml.Unmarshal(ret, supplier(cur)); err != nil {
			log.Errorf("%s marshal new content : %v", key, err)
			return
		}

		pre := c.holders[key].Load().(*Tenant)
		et := c.pathInfo.ConfigEventMapping[key]
		event := c.pathInfo.BuildEventMapping[et](pre, cur)
		log.Infof("%s receive change event : %#v", key, event)

		c.observers.notify(et, event)
		c.holders[key].Store(cur)
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
func (c *center) Subscribe(ctx context.Context, et EventType, f callback) context.CancelFunc {

	return c.observers.add(et, f)
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
