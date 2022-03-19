//
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

package resource

import (
	"encoding/json"
	"fmt"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/third_party/pools"
)

var dataSourceManager *DataSourceManager

type DataSourceManager struct {
	dataSources        []*config.DataSource
	masterResourcePool map[string]*pools.ResourcePool
	slaveResourcePool  map[string]*pools.ResourcePool
	metaResourcePool   map[string]*pools.ResourcePool
}

func InitDataSourceManager(dataSources []*config.DataSource, factory func(config json.RawMessage) pools.Factory) {
	masterResourcePool := make(map[string]*pools.ResourcePool, 0)
	slaveResourcePool := make(map[string]*pools.ResourcePool, 0)
	metaResourcePool := make(map[string]*pools.ResourcePool, 0)
	initResourcePool := func(dataSourceConfig *config.DataSource) *pools.ResourcePool {
		resourcePool := pools.NewResourcePool(factory(dataSourceConfig.Conf), dataSourceConfig.Capacity,
			dataSourceConfig.MaxCapacity, dataSourceConfig.IdleTimeout, 0, nil)
		return resourcePool
	}
	for i := 0; i < len(dataSources); i++ {
		switch dataSources[i].Role {
		case config.Master:
			resourcePool := initResourcePool(dataSources[i])
			masterResourcePool[dataSources[i].Name] = resourcePool
		case config.Slave:
			resourcePool := initResourcePool(dataSources[i])
			slaveResourcePool[dataSources[i].Name] = resourcePool
		case config.Meta:
			resourcePool := initResourcePool(dataSources[i])
			metaResourcePool[dataSources[i].Name] = resourcePool
		default:
			panic(fmt.Errorf("unsupported data source type: %d", dataSources[i].Role))
		}
	}
	dataSourceManager = &DataSourceManager{
		dataSources:        dataSources,
		masterResourcePool: masterResourcePool,
		slaveResourcePool:  slaveResourcePool,
		metaResourcePool:   metaResourcePool,
	}
}

func GetDataSourceManager() *DataSourceManager {
	return dataSourceManager
}

func (manager *DataSourceManager) GetMasterResourcePool(name string) *pools.ResourcePool {
	return manager.masterResourcePool[name]
}

func (manager *DataSourceManager) GetSlaveResourcePool(name string) *pools.ResourcePool {
	return manager.slaveResourcePool[name]
}

func (manager *DataSourceManager) GetMetaResourcePool(name string) *pools.ResourcePool {
	return manager.metaResourcePool[name]
}
