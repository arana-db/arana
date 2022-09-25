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

package registry

import (
	"context"
	"fmt"
	"github.com/arana-db/arana/pkg/registry/etcd"
	"github.com/arana-db/arana/pkg/registry/store"
	"github.com/docker/distribution/registry"
	gostnet "github.com/dubbogo/gost/net"
)

import (
	"github.com/arana-db/arana/pkg/config"
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

// DoRegistry register the service
func DoRegistry(ctx context.Context, registryInstance Registry, name string, listeners []*config.Listener) error {
	serviceInstance := &ServiceInstance{Name: name}
	for _, listener := range listeners {
		serviceInstance.Endpoints = append(serviceInstance.Endpoints, listener)
	}

	return registryInstance.Register(ctx, name, serviceInstance)
}

func Init(registryConf *config.Registry) (registry.Registry, error) {
	var serviceRegistry registry.Registry
	var err error
	switch registryConf.Name {
	case store.ETCD:
		serviceRegistry, err = initEtcd(registryConf)
	case store.NACOS:
	default:
		err = errors.Errorf("Service registry not support store:%s", registryConf.Name)
	}
	if err != nil {
		err = errors.Wrap(err, "init service registry err:%v")
		log.Fatal(err.Error())
		return nil, err
	}
	return serviceRegistry, nil
}

func initEtcd(registryConf *config.Registry) (registry.Registry, error) {
	etcdAddr, ok := registryConf.Options["endpoints"].(string)
	if !ok {
		return nil, fmt.Errorf("service registry init etcd error because get endpoints of options :%v", registryConf.Options)
	}

	serverAddr, err := gostnet.GetLocalIP()
	if !ok {
		return nil, fmt.Errorf("service registry init etcd error because get local host err:%v", err)
	}

	rootPath, ok := registryConf.Options["root_path"].(string)
	if !ok {
		return nil, fmt.Errorf("service registry init etcd error because get root_path of options :%v", registryConf.Options)
	}

	serviceRegistry, err := etcd.NewEtcdV3Registry(serverAddr, rootPath, []string{etcdAddr}, nil)
	if err != nil {
		return nil, fmt.Errorf("service registry init etcd error because err: :%v", err)
	}
	return serviceRegistry, nil
}

func initNacos(registryConf *config.Registry) {

}
