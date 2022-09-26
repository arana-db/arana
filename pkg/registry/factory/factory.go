package factory

import (
	"context"
	"fmt"
	"time"
)

import (
	gostnet "github.com/dubbogo/gost/net"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/registry"
	"github.com/arana-db/arana/pkg/registry/etcd"
	"github.com/arana-db/arana/pkg/registry/store"
	"github.com/arana-db/arana/pkg/util/log"
)

// DoRegistry register the service
func DoRegistry(ctx context.Context, registryInstance registry.Registry, name string, listeners []*config.Listener) error {
	serviceInstance := &registry.ServiceInstance{Name: name}
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

func Init(registryConf *config.Registry) (registry.Registry, error) {
	var serviceRegistry registry.Registry
	var err error
	switch registryConf.Name {
	case store.ETCD:
		serviceRegistry, err = initEtcd(registryConf)
	case store.NACOS:
	default:
		err = errors.Errorf("Service registry not support store:%s", registryConf.Name)
	}
	if err != nil {
		err = errors.Wrap(err, "init service registry err:%v")
		log.Fatal(err.Error())
		return nil, err
	}
	return serviceRegistry, nil
}

func initEtcd(registryConf *config.Registry) (registry.Registry, error) {
	etcdAddr, ok := registryConf.Options["endpoints"].(string)
	if !ok {
		return nil, fmt.Errorf("service registry init etcd error because get endpoints of options :%v", registryConf.Options)
	}

	serverAddr, err := gostnet.GetLocalIP()
	if err != nil {
		return nil, fmt.Errorf("service registry init etcd error because get local host err:%v", err)
	}

	rootPath := registryConf.RootPath
	serviceRegistry, err := etcd.NewEtcdV3Registry(serverAddr, rootPath, []string{etcdAddr}, time.Minute, nil)
	if err != nil {
		return nil, fmt.Errorf("service registry init etcd error because err: :%v", err)
	}
	return serviceRegistry, nil
}

func initNacos(registryConf *config.Registry) {

}
