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
	"context"
	"sync/atomic"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/runtime"
)

const (
	lockGraphSql = `
SELECT a.trx_xid, a.trx_mysql_thread_id AS thread_id, a.trx_started, a.trx_rows_modified, b.trx_xid AS blocking_trx
FROM information_schema.innodb_lock_waits lock_info
	JOIN information_schema.innodb_trx a
	JOIN information_schema.innodb_trx b
	ON lock_info.requesting_trx_id = a.trx_id
		AND lock_info.blocking_trx_id = b.trx_id
		AND a.trx_xa_type = 'external'
		AND b.trx_xa_type = 'external';
`
)

// NewXADeadLockDog creates xa runtime.DeadLockDog
func NewXADeadLockDog() (runtime.DeadLockDog, error) {
	return nil, nil
}

type xaDeadLockDog struct {
	ctx         context.Context
	cancel      context.CancelFunc
	tx          runtime.CompositeTx
	timer       *time.Timer
	hasDeadLock int32
}

// Start run deadlock detection dog, can set how long the delay starts to execute
func (xd *xaDeadLockDog) Start(ctx context.Context, delay time.Duration, tx runtime.CompositeTx) {
	xd.ctx, xd.cancel = context.WithCancel(ctx)
	xd.tx = tx
	xd.timer = time.AfterFunc(delay, xd.runCheck)
}

func (xd *xaDeadLockDog) runCheck() {
	select {
	case <-xd.ctx.Done():
	default:
		//	Execute SQL, query the current transaction status of each physical node,
		//	and describe a graph to judge whether there is a deadlock situation
	}
}

// HasDeadLock tx deadlock is occur
func (xd *xaDeadLockDog) HasDeadLock() bool {
	return atomic.LoadInt32(&xd.hasDeadLock) == 1
}

// Cancel stop run deadlock detection dog
func (xd *xaDeadLockDog) Cancel() {
	xd.timer.Stop()
	xd.cancel()
}
