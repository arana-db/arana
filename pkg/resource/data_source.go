package resource

import (
	"encoding/json"
	"fmt"
)

import (
	"github.com/dubbogo/kylin/pkg/config"
	"github.com/dubbogo/kylin/third_party/pools"
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
			dataSourceConfig.MaxCapacity, dataSourceConfig.IdleTimeout, 1, nil)
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
