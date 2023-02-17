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
