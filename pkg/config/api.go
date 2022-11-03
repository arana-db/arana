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
	"io"
	"path/filepath"
	"sync"
)

type (
	// ProtocolType protocol type enum
	ProtocolType int32

	// PathKey config path key type
	PathKey string

	// ConfigItem
	ConfigItem string
)

const (
	_rootPathTemp = "/%s/%s/"
)

var (
	DefaultRootPath    PathKey
	DefaultTenantsPath PathKey
)

func initPath(root, version string) {
	if root == "" {
		root = "arana-db"
	}
	if version == "" {
		version = "1.0"
	}
	DefaultRootPath = PathKey(fmt.Sprintf(_rootPathTemp, root, version))
	DefaultTenantsPath = PathKey(filepath.Join(string(DefaultRootPath), "tenants"))
}

const (
	Http ProtocolType = iota
	MySQL
)

const (
	_            DataSourceType = ""
	DBMySQL      DataSourceType = "mysql"
	DBPostgreSQL DataSourceType = "postgresql"
)

const (
	ConfigItemSpec         = "spec"
	ConfigItemUsers        = "users"
	ConfigItemClusters     = "clusters"
	ConfigItemShardingRule = "sharding_rule"
	ConfigItemNodes        = "nodes"
	ConfigItemShadowRule   = "shadow_rule"
)

var (
	slots = make(map[string]StoreOperator)

	storeOperate StoreOperator

	once sync.Once
)

// Register register store plugin
func Register(s StoreOperator) error {
	if _, ok := slots[s.Name()]; ok {
		return fmt.Errorf("StoreOperator=[%s] already exist", s.Name())
	}

	slots[s.Name()] = s
	return nil
}

type (
	EventCallback func(e Event)

	SubscribeResult struct {
		EventChan <-chan Event
		Cancel    context.CancelFunc
	}

	subscriber struct {
		watch EventCallback
		ctx   context.Context
	}

	Options struct {
		StoreName string                 `yaml:"name"`
		RootPath  string                 `yaml:"root_path"`
		Options   map[string]interface{} `yaml:"options"`
	}

	// TenantOperator actions specific to tenant spaces
	TenantOperator interface {
		io.Closer

		// ListTenants lists all tenants
		ListTenants() []string

		// CreateTenant creates tenant
		CreateTenant(string) error

		// RemoveTenant removes tenant
		RemoveTenant(string) error

		// CreateTenantUser creates a user.
		CreateTenantUser(tenant, username, password string) error

		// UpsertNode creates a node, or updates a node.
		UpsertNode(tenant, node, name, host string, port int, username, password, database, weight string) error

		// RemoveNode removes a node.
		RemoveNode(tenant, name string) error

		// Subscribe subscribes tenants change
		Subscribe(ctx context.Context, c EventCallback) context.CancelFunc
	}

	// Center Configuration center for each tenant, tenant-level isolation
	Center interface {
		io.Closer
		ConfigWriter
		ConfigReader
		ConfigWatcher
		// Tenant tenant info
		Tenant() string
	}

	ConfigReader interface {
		io.Closer
		// LoadAll loads the full Tenant configuration, the first time it will be loaded remotely,
		// and then it will be directly assembled from the cache layer
		LoadAll(ctx context.Context) (*Tenant, error)
		// Load loads the full Tenant configuration, the first time it will be loaded remotely,
		// and then it will be directly assembled from the cache layer
		Load(ctx context.Context, item ConfigItem) (*Tenant, error)
	}

	ConfigWriter interface {
		io.Closer
		// Import imports the configuration information of a tenant
		Import(ctx context.Context, cfg *Tenant) error
		// Write imports the configuration information of a tenant
		Write(ctx context.Context, item ConfigItem, cfg *Tenant) error
	}

	ConfigWatcher interface {
		io.Closer
		// Subscribe subscribes to all changes of an event by EventType
		Subscribe(ctx context.Context, et EventType, c EventCallback) (context.CancelFunc, error)
	}

	// StoreOperator config storage related plugins
	StoreOperator interface {
		io.Closer
		// Init plugin initialization
		Init(options map[string]interface{}) error
		// Save save a configuration data
		Save(key PathKey, val []byte) error
		// Get get a configuration
		Get(key PathKey) ([]byte, error)
		// Watch Monitor changes of the key
		Watch(key PathKey) (<-chan []byte, error)
		// Name plugin name
		Name() string
	}
)
