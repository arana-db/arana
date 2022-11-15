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
	"fmt"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/registry/base"
	"github.com/arana-db/arana/pkg/registry/etcd"
	"github.com/arana-db/arana/pkg/util/log"
)

func InitDiscovery(storeType string, basePath string, servicePath string, storeAddrs []string) (base.Discovery, error) {
	var serviceDiscovery base.Discovery
	var err error
	switch storeType {
	case base.ETCD:
		serviceDiscovery, err = initEtcdDiscovery(basePath, servicePath, storeAddrs)
	case base.NACOS:
	default:
		err = errors.Errorf("Service registry not support store:%s", storeType)
	}

	if err != nil {
		err = errors.Wrap(err, "init service registry err:%v")
		log.Fatal(err.Error())
		return nil, err
	}
	return serviceDiscovery, nil
}

func initEtcdDiscovery(basePath string, servicePath string, storeAddrs []string) (base.Discovery, error) {
	if len(storeAddrs) == 0 {
		return nil, fmt.Errorf("service discovery init etcd error because get endpoints nil :%v", storeAddrs)
	}

	serviceDiscovery, err := etcd.NewEtcdV3Discovery(basePath, servicePath, storeAddrs, nil)
	if err != nil {
		return nil, fmt.Errorf("service discovery init etcd error because err: :%v", err)
	}

	return serviceDiscovery, nil
}

func initNacosDiscovery(basePath string, servicePath string, storeAddrs []string) (base.Discovery, error) {
	return nil, nil
}
