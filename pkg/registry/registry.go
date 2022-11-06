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
)

import (
	gostnet "github.com/dubbogo/gost/net"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/registry/base"
	"github.com/arana-db/arana/pkg/registry/etcd"
	"github.com/arana-db/arana/pkg/util/log"
)

// DoRegistry register the service
func DoRegistry(ctx context.Context, registryInstance base.Registry, name string, listeners []*config.Listener) error {
	serviceInstance := &base.ServiceInstance{Name: name}
	serverAddr, err := gostnet.GetLocalIP()
	if err != nil {
		return fmt.Errorf("service registry register error because get local host err:%v", err)
	}
	for _, listener := range listeners {
		tmpLister := *listener
		if tmpLister.SocketAddress.Address == "0.0.0.0" || tmpLister.SocketAddress.Address == "127.0.0.1" {
			tmpLister.SocketAddress.Address = serverAddr
		}
		serviceInstance.Endpoints = append(serviceInstance.Endpoints, &tmpLister)
	}

	return registryInstance.Register(ctx, name, serviceInstance)
}

func InitRegistry(registryConf *config.Registry) (base.Registry, error) {
	var serviceRegistry base.Registry
	var err error
	switch registryConf.Name {
	case base.ETCD:
		serviceRegistry, err = initEtcdRegistry(registryConf)
	case base.NACOS:
		serviceRegistry, err = initNacosRegistry(registryConf)
	default:
		err = errors.Errorf("Service registry not support store:%s", registryConf.Name)
	}
	if err != nil {
		err = errors.Wrap(err, "init service registry err:%v")
		log.Fatal(err.Error())
		return nil, err
	}
	log.Infof("init %s registry success", registryConf.Name)
	return serviceRegistry, nil
}

func initEtcdRegistry(registryConf *config.Registry) (base.Registry, error) {
	etcdAddr, ok := registryConf.Options["endpoints"].(string)
	if !ok {
		return nil, fmt.Errorf("service registry init etcd error because get endpoints of options :%v", registryConf.Options)
	}

	serverAddr, err := gostnet.GetLocalIP()
	if err != nil {
		return nil, fmt.Errorf("service registry init etcd error because get local host err:%v", err)
	}

	rootPath := registryConf.RootPath
	serviceRegistry, err := etcd.NewEtcdV3Registry(serverAddr, rootPath, []string{etcdAddr}, nil)
	if err != nil {
		return nil, fmt.Errorf("service registry init etcd error because err: :%v", err)
	}

	log.Infof("registry %s etcd registry success", registryConf.Name)

	return serviceRegistry, nil
}

func initNacosRegistry(registryConf *config.Registry) (base.Registry, error) {
	return nil, nil
}
