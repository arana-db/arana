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

package base

import (
	"context"
	"fmt"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

// Backend represents a KV Store Backend
const (
	NACOS string = "nacos"
	ETCD  string = "etcd"
)

// ServiceInstance is an instance of a service in a discovery system.
type ServiceInstance struct {
	// ID is the unique instance ID as registered.
	ID string `json:"id"`
	// Name is the service name as registered.
	Name string `json:"name"`
	// Version is the version of the compiled.
	Version string `json:"version"`
	// Endpoint addresses of the service instance.
	Endpoints []*config.Listener
}

func (p ServiceInstance) String() string {
	return fmt.Sprintf("Service instance: id:%s, name:%s, version:%s, endpoints:%s", p.ID, p.Name, p.Version, p.Endpoints)
}

type Registry interface {
	Register(ctx context.Context, name string, serviceInstance *ServiceInstance) error
	Unregister(ctx context.Context, name string) error
	UnregisterAllService(ctx context.Context) error
}

type Discovery interface {
	GetServices() []*ServiceInstance
	WatchService() <-chan []*ServiceInstance
	Close()
}
