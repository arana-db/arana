package factory

type Registry interface {
	Register(ctx context.Context, name string, serviceInstance *ServiceInstance) error
	Unregister(ctx context.Context, name string) error
	UnregisterAllService(ctx context.Context) error
}

type Discovery interface {
	GetServices() []*ServiceInstance
	WatchService() chan *ServiceInstance
	Close()
}
