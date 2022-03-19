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

package boot

import (
	"context"
	"time"
)

import (
	etcdv3 "github.com/dubbogo/gost/database/kv/etcd/v3"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
)

var _ Discovery = (*EtcdProvider)(nil)

type EtcdProvider struct {
	rootPath string
	client   *etcdv3.Client
}

func (ep *EtcdProvider) ListTenants(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) ListListeners(ctx context.Context) ([]*config.Listener, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) ListFilters(ctx context.Context) ([]*config.Filter, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) ListClusters(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) ListGroups(ctx context.Context, cluster string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) ListNodes(ctx context.Context, cluster, group string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) ListTables(ctx context.Context, cluster string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) GetNode(ctx context.Context, cluster, group, node string) (*config.Node, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) GetTable(ctx context.Context, cluster, table string) (*rule.VTable, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) GetTenant(ctx context.Context, tenant string) (*config.Tenant, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) GetCluster(ctx context.Context, cluster string) (*Cluster, error) {
	//TODO implement me
	panic("implement me")
}

func (ep *EtcdProvider) Init(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func NewEtcdProvider(rootPath string, endpoints ...string) (Discovery, error) {
	c, err := etcdv3.NewConfigClientWithErr(
		etcdv3.WithName(etcdv3.RegistryETCDV3Client),
		etcdv3.WithTimeout(10*time.Second),
		etcdv3.WithEndpoints(endpoints...),
	)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect etcd")
	}

	return &EtcdProvider{
		rootPath: rootPath,
		client:   c,
	}, nil
}
