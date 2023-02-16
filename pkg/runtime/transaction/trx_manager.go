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

package transaction

import (
	"errors"
	"sync"
)

import (
	"github.com/arana-db/arana/pkg/runtime/namespace"
)

var (
	ErrorTrxManagerNotInitialize = errors.New("TrxManager not initialize")
	ErrorNotFoundTargetTenant    = errors.New("tenant not found")
)

var (
	lock    sync.RWMutex
	trxMgrs map[string]*TrxManager
)

// InitTrxManager inits TxBottomMaker
func InitTrxManager(tenant string) error {
	lock.Lock()
	defer lock.Unlock()

	if _, ok := trxMgrs[tenant]; ok {
		return nil
	}

	ns := namespace.Load(tenant)
	if ns == nil {
		return ErrorNotFoundTargetTenant
	}

	trxLog := &TxLogManager{sysDB: ns.SysDB()}
	trxBottomMaker := &TxBottomMaker{tm: trxLog}

	trxMgrs[tenant] = &TrxManager{
		trxLog:         trxLog,
		trxBottomMaker: trxBottomMaker,
	}

	return nil
}

// GetTrxManager returns *TrxManager
func GetTrxManager(tenant string) (*TrxManager, error) {
	lock.RLock()
	defer lock.RUnlock()
	if len(trxMgrs) == 0 {
		return nil, ErrorTrxManagerNotInitialize
	}
	return trxMgrs[tenant], nil
}

type TrxManager struct {
	trxLog         *TxLogManager
	trxBottomMaker *TxBottomMaker
}
