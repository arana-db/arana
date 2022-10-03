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
	"sync"
	"sync/atomic"
)

import (
	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

type (
	consumer          func(key PathKey, ret []byte)
	ConfigWatcherTest configWatcher
)

func NewConfigWatcher(tenant string, storeOperate StoreOperator, pathInfo *PathInfo) (*configWatcher, error) {

	holders := map[PathKey]*atomic.Value{}
	for k := range pathInfo.ConfigKeyMapping {
		holders[k] = &atomic.Value{}
		holders[k].Store(NewEmptyTenant())
	}

	c := &configWatcher{
		tenant:       tenant,
		storeOperate: storeOperate,
		pathInfo:     pathInfo,
		holders:      holders,
		watchCancels: make([]context.CancelFunc, 0, 4),
		observers: &observerBucket{
			observers: map[EventType][]*subscriber{},
		},
	}

	consumer := func(key PathKey, ret []byte) {
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
	if err := c.watchFromStore(consumer); err != nil {
		return nil, err
	}

	return c, nil
}

type configWatcher struct {
	tenant string

	storeOperate StoreOperator
	pathInfo     *PathInfo
	once         sync.Once

	holders map[PathKey]*atomic.Value

	observers    *observerBucket
	watchCancels []context.CancelFunc
}

func (c *configWatcher) Close() error {
	for i := range c.watchCancels {
		c.watchCancels[i]()
	}
	return nil
}

// Subscribe
func (c *configWatcher) Subscribe(ctx context.Context, et EventType, f EventCallback) (context.CancelFunc, error) {
	return c.observers.add(et, f), nil
}

func (c *configWatcher) watchFromStore(consumer consumer) error {
	cancels := make([]context.CancelFunc, 0, len(c.pathInfo.ConfigKeyMapping))

	for key := range c.pathInfo.ConfigKeyMapping {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		ch, err := c.storeOperate.Watch(key)
		if err != nil {
			return err
		}

		go c.watchKey(ctx, key, ch, consumer)
	}

	c.watchCancels = cancels
	return nil
}

func (c *configWatcher) watchKey(ctx context.Context, key PathKey, ch <-chan []byte, consumer consumer) {
	for {
		select {
		case ret, ok := <-ch:
			if !ok {
				return
			}
			consumer(key, ret)
		case <-ctx.Done():
			return
		}
	}
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

func (b *observerBucket) add(et EventType, f EventCallback) context.CancelFunc {
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
