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

package main

import (
	"sync"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/registry"
	"github.com/arana-db/arana/pkg/registry/base"
	"github.com/arana-db/arana/pkg/util/log"
)

func main() {
	var storeType = base.ETCD
	var basePath = "arana"
	var storeAddrs = []string{"http://127.0.0.1:2379"}

	etcdDiscovery, err := registry.InitDiscovery(storeType, basePath, "service", storeAddrs)
	if err != nil {
		log.Fatalf("Init %s discovery err:%v", storeType, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go displayWatchService(etcdDiscovery)
	go displayGetService(etcdDiscovery)
	wg.Wait()
}

func displayWatchService(etcdDiscovery base.Discovery) {
	log.Infof("watch service...")
	for service := range etcdDiscovery.WatchService() {
		log.Infof("watch service, %s", service)
	}
}

func displayGetService(etcdDiscovery base.Discovery) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, service := range etcdDiscovery.GetServices() {
			log.Infof("get service, %s", service)
		}
	}
}
