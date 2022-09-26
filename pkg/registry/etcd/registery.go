package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/registry"
	"github.com/arana-db/arana/pkg/registry/store"
	"github.com/arana-db/arana/pkg/util/log"
)

// EtcdV3Registry implements etcd registry.
type EtcdV3Registry struct {
	BasePath       string
	ServiceAddress string
	EtcdServers    []string

	Services       []string
	metasLock      sync.RWMutex
	metas          map[string]string
	UpdateInterval time.Duration
	Expired        time.Duration

	client store.Store
	dying  chan struct{}
	done   chan struct{}
}

// NewEtcdV3Registry init etcd v3 registry
func NewEtcdV3Registry(serviceAddr, path string, etcdAddrs []string, updateInterval time.Duration, options *store.Options) (registry.Registry, error) {
	etcdRegistry := &EtcdV3Registry{
		BasePath:       path,
		ServiceAddress: serviceAddr,
		EtcdServers:    etcdAddrs,
		UpdateInterval: updateInterval,
		Expired:        updateInterval,
	}

	store.AddStore(store.ETCD, store.NewEtcdV3)
	client, err := store.NewStore(store.ETCD, etcdAddrs, options)
	if err != nil {
		log.Errorf("EtcdV3 Registry create etcdv3 client err:%v", err)
		return nil, errors.Wrap(err, "EtcdV3 Registry create etcdv3 client")
	}
	if client == nil {
		return nil, errors.New("EtcdV3 Registry create etcdv3: nil client")
	}
	etcdRegistry.client = client

	if etcdRegistry.UpdateInterval > 0 {
		ticker := time.NewTicker(etcdRegistry.UpdateInterval)
		go func() {
			defer client.Close()
			// refresh service TTL
			for {
				select {
				case <-etcdRegistry.dying:
					close(etcdRegistry.done)
					return
				case <-ticker.C:
					//set this same metrics for all services at this server
					for _, name := range etcdRegistry.Services {
						nodePath := fmt.Sprintf("%s/%s/%s", etcdRegistry.BasePath, name, etcdRegistry.ServiceAddress)
						if _, err := client.Get(context.Background(), nodePath); err != nil {
							log.Warnf("can't get data of node: %s, because of %v", nodePath, err)

							etcdRegistry.metasLock.RLock()
							meta := etcdRegistry.metas[name]
							etcdRegistry.metasLock.RUnlock()

							ttl := int64(etcdRegistry.UpdateInterval + etcdRegistry.Expired)
							err = client.Put(context.Background(), nodePath, []byte(meta), ttl)
							if err != nil {
								log.Errorf("cannot re-create etcd path %s: %v", nodePath, err)
							}
						}
					}
				}
			}
		}()
	}
	return etcdRegistry, nil
}

func (r *EtcdV3Registry) Register(ctx context.Context, name string, serviceInstance *registry.ServiceInstance) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("Register service `name` can't be empty")
	}

	nodePath := fmt.Sprintf("%s/%s/%s", r.BasePath, name, r.ServiceAddress)
	ttl := r.UpdateInterval + r.Expired
	serverInstanceBytes, err := json.Marshal(serviceInstance)
	if err != nil {
		return errors.Errorf("Register service name:%s marshal instance %v err:%v", name, serviceInstance, err)
	}

	err = r.client.Put(ctx, nodePath, serverInstanceBytes, int64(ttl))
	if err != nil {
		log.Errorf("cannot create etcd path %s: %v", nodePath, err)
		return err
	}

	r.Services = append(r.Services, name)

	r.metasLock.Lock()
	if r.metas == nil {
		r.metas = make(map[string]string)
	}
	r.metas[name] = string(serverInstanceBytes)
	r.metasLock.Unlock()
	return nil
}

// UnregisterAllService unregister all services.
func (r *EtcdV3Registry) UnregisterAllService(ctx context.Context) error {
	for _, name := range r.Services {
		nodePath := fmt.Sprintf("%s/%s/%s", r.BasePath, name, r.ServiceAddress)
		exist, err := r.client.Exists(ctx, nodePath)
		if err != nil {
			log.Errorf("cannot delete path %s: %v", nodePath, err)
			continue
		}

		if exist {
			r.client.Delete(ctx, nodePath)
			log.Infof("delete path %s", nodePath)
		}
	}

	close(r.dying)
	<-r.done
	return nil
}

// Unregister the name service
func (r *EtcdV3Registry) Unregister(ctx context.Context, name string) (err error) {
	if strings.TrimSpace(name) == "" {
		err = errors.New("Register service `name` can't be empty")
		return
	}

	nodePath := fmt.Sprintf("%s/%s/%s", r.BasePath, name, r.ServiceAddress)
	err = r.client.Delete(ctx, nodePath)
	if err != nil {
		log.Errorf("cannot create consul path %s: %v", nodePath, err)
		return err
	}

	if len(r.Services) > 0 {
		var services = make([]string, 0, len(r.Services)-1)
		for _, s := range r.Services {
			if s != name {
				services = append(services, s)
			}
		}
		r.Services = services
	}

	r.metasLock.Lock()
	if r.metas == nil {
		r.metas = make(map[string]string)
	}
	delete(r.metas, name)
	r.metasLock.Unlock()
	return nil
}
