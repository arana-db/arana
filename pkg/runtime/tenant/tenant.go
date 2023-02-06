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

package tenant

import (
	"fmt"
	"sync"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
)

var (
	lock         sync.RWMutex
	_tenantSysDB = make(map[string]*tenantSysInfo)
)

type tenantSysInfo struct {
	Conf *config.Node
	DB   proto.DB
}

func RegisterSysDB(tenant string, conf *config.Node, db proto.DB) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok := _tenantSysDB[tenant]; ok {
		return
	}

	_tenantSysDB[tenant] = &tenantSysInfo{
		Conf: conf,
		DB:   db,
	}
}

func LoadSysDB(tenant string) (proto.DB, error) {
	lock.RLock()
	defer lock.RUnlock()

	val, ok := _tenantSysDB[tenant]
	if ok {
		return nil, fmt.Errorf("cannot load sysdb: tenant=%s", tenant)
	}

	return val.DB, nil
}
