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
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/registry/base"
)

// NacosV2Discovery is a nacos service discovery.
// It always returns the registered servers in etcd.
type NacosV2Discovery struct {
	client *NacosServiceClient
}

// NewNacosV2Discovery returns a new NacosV2Discovery.
func NewNacosV2Discovery(options map[string]interface{}) (base.Discovery, error) {
	client, err := NewNacosServiceClient(options)
	if err != nil {
		return nil, err
	}
	return &NacosV2Discovery{client: client}, nil
}

// GetServices returns the servers
func (nd *NacosV2Discovery) GetServices() []*base.ServiceInstance {
	instances := nd.client.SelectAllServiceInstances()
	result := make([]*base.ServiceInstance, 0, len(instances))

	for _, instance := range instances {
		result = append(result, &base.ServiceInstance{
			ID:      instance.InstanceId,
			Name:    instance.ServiceName,
			Version: "",
			Endpoint: &config.Listener{
				ProtocolType: instance.Metadata[_protocolType],
				SocketAddress: &config.SocketAddress{
					Address: instance.Ip,
					Port:    int(instance.Port),
				},
				ServerVersion: instance.Metadata[_serverVersion],
			},
		})
	}

	return result
}

// WatchService returns a nil chan.
func (nd *NacosV2Discovery) WatchService() <-chan []*base.ServiceInstance {
	// TODO will support later
	return nil
}

func (nd *NacosV2Discovery) Close() {
	// TODO will support later
	nd.client.CloseClient()
}
