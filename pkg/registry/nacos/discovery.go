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
