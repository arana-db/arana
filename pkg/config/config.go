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
	"errors"
	"sync/atomic"
)

import (
	"gopkg.in/yaml.v3"
)

var (
	ErrorNotImplement = errors.New("not implement")
)

type (
	CenterTest = center
)

type center struct {
	tenant     string
	initialize int32

	storeOperate StoreOperator

	Reader  ConfigReader
	Writer  ConfigWriter
	Watcher ConfigWatcher
}

func NewCenter(tenant string, storeOperate StoreOperator, opts ...option) (Center, error) {
	c := &center{
		storeOperate: storeOperate,
		tenant:       tenant,
	}

	opt := &configOption{
		tenant: tenant,
	}

	for i := range opts {
		opts[i](opt)
	}

	if err := c.newConfigWriter(*opt); err != nil {
		return nil, err
	}

	if err := c.newConfigWatcher(*opt); err != nil {
		return nil, err
	}

	if err := c.newConfigReader(*opt); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *center) newConfigReader(opt configOption) error {
	if !opt.openReader {
		return nil
	}

	p := NewPathInfo(opt.tenant)

	holders := map[PathKey]*atomic.Value{}
	for k := range p.ConfigKeyMapping {
		holders[k] = &atomic.Value{}
		holders[k].Store(NewEmptyTenant())
	}

	var ret ConfigReader

	reader := &configReader{
		tenant:       opt.tenant,
		storeOperate: c.storeOperate,
		pathInfo:     p,
		holders:      holders,
	}
	ret = reader

	if opt.cacheable {
		watcher, err := NewConfigWatcher(c.tenant, c.storeOperate, NewPathInfo(opt.tenant))
		if err != nil {
			return err
		}

		ret = &cacheConfigReader{
			reader:  reader,
			watcher: watcher,
		}
	}

	c.Reader = ret
	return nil
}

func (c *center) newConfigWriter(opt configOption) error {
	if !opt.openWriter {
		return nil
	}

	writer := &configWriter{
		tenant:       opt.tenant,
		storeOperate: c.storeOperate,
		pathInfo:     NewPathInfo(opt.tenant),
	}

	c.Writer = writer
	return nil
}

func (c *center) newConfigWatcher(opt configOption) error {
	if !opt.openWatcher {
		return nil
	}

	watcher, err := NewConfigWatcher(c.tenant, c.storeOperate, NewPathInfo(opt.tenant))
	if err != nil {
		return err
	}

	c.Watcher = watcher
	return nil
}

// LoadAll loads the full Tenant configuration, the first time it will be loaded remotely,
// and then it will be directly assembled from the cache layer
func (c *center) LoadAll(ctx context.Context) (*Tenant, error) {
	if c.Reader == nil {
		return nil, ErrorNotImplement
	}

	return c.Reader.LoadAll(ctx)
}

// Load loads the full Tenant configuration, the first time it will be loaded remotely,
// and then it will be directly assembled from the cache layer
func (c *center) Load(ctx context.Context, item ConfigItem) (*Tenant, error) {
	if c.Reader == nil {
		return nil, ErrorNotImplement
	}

	return c.Reader.Load(ctx, item)
}

// ImportAll imports the configuration information of a tenant
func (c *center) ImportAll(ctx context.Context, cfg *Tenant) error {
	if c.Writer == nil {
		return ErrorNotImplement
	}

	return c.Writer.ImportAll(ctx, cfg)
}

// Write imports the configuration information of a tenant
func (c *center) Write(ctx context.Context, item ConfigItem, cfg *Tenant) error {
	if c.Writer == nil {
		return ErrorNotImplement
	}

	return c.Writer.Write(ctx, item, cfg)
}

// Subscribe subscribes to all changes of an event by EventType
func (c *center) Subscribe(ctx context.Context, et EventType, cb EventCallback) (context.CancelFunc, error) {
	if c.Watcher == nil {
		return nil, ErrorNotImplement
	}

	return c.Watcher.Subscribe(ctx, et, cb)
}

func (c *center) Close() error {
	if c.Reader != nil {
		if err := c.Reader.Close(); err != nil {
			return err
		}
	}
	if c.Writer != nil {
		if err := c.Writer.Close(); err != nil {
			return err
		}
	}
	if c.Watcher != nil {
		if err := c.Watcher.Close(); err != nil {
			return err
		}
	}
	if c.storeOperate != nil {
		if err := c.storeOperate.Close(); err != nil {
			return err
		}
	}
	return nil
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
