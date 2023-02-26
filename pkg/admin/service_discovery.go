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

package admin

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/registry/base"
)

type ServiceDiscovery interface {
	ListServices() []*ServiceInstanceDTO
}

type myServiceDiscovery struct {
	serviceDiscovery base.Discovery
}

func (mysds *myServiceDiscovery) ListServices() []*ServiceInstanceDTO {
	var (
		services = mysds.serviceDiscovery.GetServices()
		srvDTOs  = make([]*ServiceInstanceDTO, 0, len(services))
	)
	for _, srv := range services {
		endpoints := make([]*config.Listener, len(srv.Endpoints))
		copy(endpoints, srv.Endpoints)
		srvDTOs = append(srvDTOs, &ServiceInstanceDTO{
			ID:        srv.ID,
			Name:      srv.Name,
			Version:   srv.Version,
			Endpoints: endpoints,
		})
	}
	return srvDTOs
}
