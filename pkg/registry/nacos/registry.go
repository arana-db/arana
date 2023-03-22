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

package nacos

import (
	"context"
)

import (
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

import (
	"github.com/arana-db/arana/pkg/registry/base"
)

const (
	_protocolType  = "protocol_type"
	_serverVersion = "server_version"
)

type NacosV2Registry struct {
	client *NacosServiceClient
}

func NewNacosV2Registry(options map[string]interface{}) (base.Registry, error) {
	client, err := NewNacosServiceClient(options)
	if err != nil {
		return nil, err
	}
	return &NacosV2Registry{client: client}, nil
}

func (ng *NacosV2Registry) Register(ctx context.Context, serviceInstance *base.ServiceInstance) error {
	metadata := make(map[string]string)
	metadata[_protocolType] = serviceInstance.Endpoint.ProtocolType
	metadata[_serverVersion] = serviceInstance.Endpoint.ServerVersion
	instance := vo.RegisterInstanceParam{
		Ip:          serviceInstance.Endpoint.SocketAddress.Address,
		Port:        uint64(serviceInstance.Endpoint.SocketAddress.Port),
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		ServiceName: serviceInstance.Name,
		// TODO make GroupName and ClusterName configurable
		GroupName: "DEFAULT_GROUP",
		Metadata:  metadata,
		Ephemeral: true,
	}
	ok, err := ng.client.RegisterInstance(instance)
	if err != nil {
		return err
	}
	if !ok {
		logger.Warnf("Register service %s failed.", serviceInstance)
	}
	return nil
}

func (ng *NacosV2Registry) Unregister(ctx context.Context, name string) error {
	instance, err := ng.client.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{ServiceName: name})
	if err != nil {
		return err
	}
	_, err = ng.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          instance.Ip,
		Port:        instance.Port,
		ServiceName: instance.ServiceName,
		Ephemeral:   true,
	})
	if err != nil {
		return err
	}
	return nil
}

func (ng *NacosV2Registry) UnregisterAllService(ctx context.Context) error {
	instances := ng.client.SelectAllServiceInstances()
	for _, inst := range instances {
		flag, err := ng.client.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          inst.Ip,
			Port:        inst.Port,
			Cluster:     inst.ClusterName,
			ServiceName: inst.ServiceName,
			Ephemeral:   true,
		})
		if err != nil {
			return err
		}
		if !flag {
			logger.Infof("Deregister instance %s failed", inst)
		}
	}
	return nil
}
