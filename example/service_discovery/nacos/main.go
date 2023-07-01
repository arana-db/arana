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
	"github.com/arana-db/arana/pkg/registry"
	"github.com/arana-db/arana/pkg/registry/base"
	"github.com/arana-db/arana/pkg/util/log"
)

func main() {
	storeType := base.NACOS
	options := make(map[string]interface{})
	options["endpoints"] = "127.0.0.1:8848"
	options["scheme"] = "http"
	options["username"] = "nacos"
	options["password"] = "nacos"

	nacosDiscovery, err := registry.InitDiscovery(base.NACOS, options)
	if err != nil {
		log.Fatalf("Init %s discovery err:%v", storeType, err)
		return
	}

	nacosDiscovery.GetServices()
}
