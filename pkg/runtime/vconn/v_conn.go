//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package vconn

import (
	"context"
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/mysql"
	"github.com/dubbogo/arana/pkg/proto"
)

type VConn struct {
	bcMap map[string]*mysql.BackendConnection
	rw    *sync.RWMutex
}

func NewVConn(m map[string]*mysql.BackendConnection) proto.VConn {
	return &VConn{
		bcMap: m,
		rw:    &sync.RWMutex{},
	}
}

func (v *VConn) Query(ctx context.Context, db string, query string, args ...interface{}) (proto.Rows, error) {
	bc, ok := v.getBackendConnection(db)
	if !ok {
		return nil, errors.Errorf("database: %s not exit, while doing Query", db)
	}
	// TODO handle warnings
	res, _, err := bc.PrepareQueryArgs(query, args)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: res.Rows}, nil
}

func (v *VConn) Exec(ctx context.Context, db string, query string, args ...interface{}) (proto.Result, error) {
	bc, ok := v.getBackendConnection(db)
	if !ok {
		return nil, errors.Errorf("database: %s not exit, while doing Exec", db)
	}
	// TODO handle warnings
	res, _, err := bc.PrepareExecuteArgs(query, args)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (v *VConn) getBackendConnection(db string) (*mysql.BackendConnection, bool) {
	v.rw.RLock()
	bc, ok := v.bcMap[db]
	v.rw.RUnlock()
	return bc, ok
}
