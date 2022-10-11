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
	"sync"
)

import (
	"github.com/pkg/errors"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	_ TenantOperator = (*tenantOperate)(nil)
)

// NewTenantOperator create a tenant data operator
func NewTenantOperator(op StoreOperator) (TenantOperator, error) {
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
	op   StoreOperator
	lock sync.RWMutex

	tenants   map[string]struct{}
	observers *observerBucket

	cancels []context.CancelFunc
}

func (tp *tenantOperate) Subscribe(ctx context.Context, c EventCallback) context.CancelFunc {
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
				return
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
			return errors.Wrapf(err, "create tenant resource : %s", i)
		}
	}

	return nil
}

func (tp *tenantOperate) CreateTenantUser(tenant, username, password string) error {
	p := NewPathInfo(tenant)

	prev, err := tp.op.Get(p.DefaultConfigDataUsersPath)
	if err != nil {
		return errors.WithStack(err)
	}

	var users Users
	if err := yaml.Unmarshal(prev, &users); err != nil {
		return errors.WithStack(err)
	}
	var found bool
	for i := 0; i < len(users); i++ {
		if users[i].Username == username {
			users[i].Password = password
			found = true
			break
		}
	}

	if !found {
		users = append(users, &User{
			Username: username,
			Password: password,
		})
	}

	b, err := yaml.Marshal(users)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := tp.op.Save(p.DefaultConfigDataUsersPath, b); err != nil {
		return errors.WithStack(err)
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
