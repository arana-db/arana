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

package runtime

import (
	"context"
)

import (
	gxnet "github.com/dubbogo/gost/net"
)

import (
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/third_party/pools"
)

type BackendResourcePool pools.ResourcePool

// Get will return the next available resource and convert to mysql.BackendConnection.
// If available less than 1, it will make capacity increase by 1.
// If the net connection check failure, it will take again.
func (bcp *BackendResourcePool) Get(ctx context.Context) (*mysql.BackendConnection, error) {
	rp := (*pools.ResourcePool)(bcp)
	if rp.Available() < 1 {
		_ = rp.SetCapacity(int(rp.Capacity()) + 1)
	}
	res, err := rp.Get(ctx)
	if err != nil {
		return nil, err
	}

	conn := res.(*mysql.BackendConnection).GetDatabaseConn()

	if err := gxnet.ConnCheck(conn.GetNetConn()); err != nil {
		rp.Put(nil)
		res, err = rp.Get(ctx)
	}
	return res.(*mysql.BackendConnection), nil
}
