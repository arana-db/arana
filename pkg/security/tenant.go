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

package security

import (
	"sync"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

var _ TenantManager = (*simpleTenantManager)(nil)

// TenantManager represents the manager of tenants.
type TenantManager interface {
	// GetTenants returns all tenants.
	GetTenants() []string
	// GetUser returns user by tenant and username.
	GetUser(tenant string, username string) (*config.User, bool)
	// GetUsers returns users by tenant
	GetUsers(tenant string) ([]*config.User, bool)
	// GetClusters returns cluster names.
	GetClusters(tenant string) []string
	// GetTenantsOfCluster returns all tenants of cluster.
	GetTenantsOfCluster(cluster string) []string
	// PutUser puts a user into tenant.
	PutUser(tenant string, user *config.User)
	// RemoveUser removes a user from tenant.
	RemoveUser(tenant string, username string)
	// PutCluster puts a cluster into tenant.
	PutCluster(tenant string, cluster string)
	// RemoveCluster removes a cluster from tenant.
	RemoveCluster(tenant string, cluster string)
	// SetSupervisor gets supervisor info
	SetSupervisor(supervisor *config.User)
	// GetSupervisor gets supervisor info
	GetSupervisor() *config.User
}

type tenantItem struct {
	clusters map[string]struct{}
	users    map[string]*config.User
}

type simpleTenantManager struct {
	sync.RWMutex
	supervisor *config.User
	tenants    map[string]*tenantItem
}

func (st *simpleTenantManager) GetTenants() []string {
	st.RLock()
	defer st.RUnlock()
	tenants := make([]string, 0, len(st.tenants))
	for k := range st.tenants {
		tenants = append(tenants, k)
	}
	return tenants
}

func (st *simpleTenantManager) GetUser(tenant string, username string) (*config.User, bool) {
	st.RLock()
	defer st.RUnlock()
	exist, ok := st.tenants[tenant]
	if !ok {
		return nil, false
	}
	user, ok := exist.users[username]
	return user, ok
}

func (st *simpleTenantManager) GetUsers(tenant string) ([]*config.User, bool) {
	st.RLock()
	defer st.RUnlock()
	var users = []*config.User{}
	exist, ok := st.tenants[tenant]
	if !ok {
		return nil, false
	} else {
		for _, v := range exist.users {
			users = append(users, v)
		}
	}
	return users, ok
}

func (st *simpleTenantManager) GetClusters(tenant string) []string {
	st.RLock()
	defer st.RUnlock()
	exist, ok := st.tenants[tenant]
	if !ok {
		return nil
	}

	clusters := make([]string, 0, len(exist.clusters))
	for k := range exist.clusters {
		clusters = append(clusters, k)
	}
	return clusters
}

func (st *simpleTenantManager) GetTenantsOfCluster(cluster string) []string {
	st.RLock()
	defer st.RUnlock()
	var ret []string
	for k, v := range st.tenants {
		if _, ok := v.clusters[cluster]; ok {
			ret = append(ret, k)
		}
	}
	return ret
}

func (st *simpleTenantManager) PutUser(tenant string, user *config.User) {
	st.Lock()
	defer st.Unlock()
	current, ok := st.tenants[tenant]
	if !ok {
		current = &tenantItem{
			clusters: make(map[string]struct{}),
			users:    make(map[string]*config.User),
		}
		st.tenants[tenant] = current
	}
	current.users[user.Username] = user
}

func (st *simpleTenantManager) RemoveUser(tenant string, username string) {
	st.Lock()
	defer st.Unlock()

	exist, ok := st.tenants[tenant]
	if !ok {
		return
	}

	delete(exist.users, username)
}

func (st *simpleTenantManager) PutCluster(tenant string, cluster string) {
	st.Lock()
	defer st.Unlock()

	current, ok := st.tenants[tenant]
	if !ok {
		current = &tenantItem{
			clusters: make(map[string]struct{}),
			users:    make(map[string]*config.User),
		}
		st.tenants[tenant] = current
	}

	current.clusters[cluster] = struct{}{}
}

func (st *simpleTenantManager) RemoveCluster(tenant string, cluster string) {
	st.Lock()
	defer st.Unlock()

	exist, ok := st.tenants[tenant]
	if !ok {
		return
	}
	delete(exist.clusters, cluster)
}

func (st *simpleTenantManager) SetSupervisor(supervisor *config.User) {
	st.Lock()
	defer st.Unlock()

	st.supervisor = supervisor
}

func (st *simpleTenantManager) GetSupervisor() *config.User {
	st.Lock()
	defer st.Unlock()

	return st.supervisor
}

var (
	_defaultTenantManager     TenantManager
	_defaultTenantManagerOnce sync.Once
)

func newSimpleTenantManager() *simpleTenantManager {
	return &simpleTenantManager{
		tenants: make(map[string]*tenantItem),
	}
}

func DefaultTenantManager() TenantManager {
	_defaultTenantManagerOnce.Do(func() {
		_defaultTenantManager = newSimpleTenantManager()
	})
	return _defaultTenantManager
}
