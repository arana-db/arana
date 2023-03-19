package nacos

import (
	"strings"
)

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/registry/base"
	u_conf "github.com/arana-db/arana/pkg/util/config"
)

// NacosV2Discovery is a nacos service discovery.
// It always returns the registered servers in etcd.
type NacosV2Discovery struct {
	client naming_client.INamingClient

	NamespaceId string
	Servers     []string // ip+port, like 127.0.0.1:8848
}

// NewNacosV2Discovery returns a new NacosV2Discovery.
func NewNacosV2Discovery(options map[string]interface{}) (base.Discovery, error) {
	discovery := &NacosV2Discovery{}
	if val, ok := options[u_conf.NamespaceIdKey]; ok {
		discovery.NamespaceId = val.(string)
	}

	if val, ok := options[u_conf.Server]; ok {
		discovery.Servers = strings.Split(val.(string), u_conf.ServerSplit)
	}

	client, err := u_conf.NewNacosV2NamingClient(options)
	if err != nil {
		return nil, err
	}
	return &NacosV2Discovery{client: client}, nil
}

// GetServices returns the servers
func (nd *NacosV2Discovery) GetServices() []*base.ServiceInstance {
	// TODO need to deal with cases that the service count is more than pageSize
	services, err := nd.client.GetAllServicesInfo(vo.GetAllServiceInfoParam{PageNo: 1, PageSize: 10})
	if err != nil {
		logger.Warnf("Could not get all service info from nacos "+
			"with namespace %s and servers %s because %s", nd.NamespaceId, nd.Servers, err)
		return nil
	}

	result := make([]*base.ServiceInstance, 0, services.Count)
	for _, srv := range services.Doms {
		instances, err := nd.client.SelectAllInstances(vo.SelectAllInstancesParam{ServiceName: srv})
		if err != nil {
			logger.Warnf("Could not get all instance info from nacos "+
				"with namespace %s、servers %s and service %s because %s", nd.NamespaceId, nd.Servers, srv, err)
			return nil
		}
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
