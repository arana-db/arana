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

package snowflake

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"go.uber.org/zap"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/identity"
	"github.com/arana-db/arana/pkg/util/log"
)

const SequencePluginName = "snowflake"

func init() {
	proto.RegisterSequence(SequencePluginName, func() proto.EnhancedSequence {
		return &snowflakeSequence{}
	})
}

const (
	_initTableSql = `
	CREATE TABLE IF NOT EXISTS __arana_snowflake_sequence (
	    id int AUTO_INCREMENT COMMENT 'primary key',
		work_id int COMMENT 'snowflake work id',
		node_id varchar(255) NOT NULL COMMENT 'unique id of the node, eg. ip/name/uuid',
		table_name varchar(255) NOT NULL COMMENT 'arana logic table name',
	    renew_time datetime NOT NULL COMMENT 'node renew time',
		PRIMARY KEY (id),
        UNIQUE KEY(work_id, table_name),
		UNIQUE KEY(node_id, table_name)
	) ENGINE = InnoDB;
	`

	//_setWorkId inserts new work_id
	_setWorkId = `REPLACE INTO __arana_snowflake_sequence(work_id, node_id, table_name, renew_time) VALUE (?, ?, ?, now())`

	//_selectSelfWorkIdWithXLock find self already has work-id
	_selectSelfWorkIdWithXLock = `SELECT work_id FROM __arana_snowflake_sequence WHERE table_name = ? AND node_id = ? FOR UPDATE`

	//_selectMaxWorkIdWithXLock select MAX(work_id) this table
	_selectMaxWorkIdWithXLock = `SELECT MAX(work_id) FROM __arana_snowflake_sequence WHERE table_name = ? FOR UPDATE`

	//_selectFreeWorkIdWithXLock find one free work-id depend on renew_time over 10 min not update，
	_selectFreeWorkIdWithXLock = `SELECT MAX(work_id) FROM __arana_snowflake_sequence WHERE table_name = ? AND renew_time < DATE_SUB(NOW(), INTERVAL 10 MINUTE) FOR UPDATE`

	//_updateWorkIdOwner update work-id relation node_id info and reuse work-id
	_updateWorkIdOwner = `UPDATE __arana_snowflake_sequence SET node_id = ?, renew_time = now() WHERE table_name = ? AND work_id = ?`

	//_keepaliveNode do keep alive for node
	_keepaliveNode = `UPDATE __arana_snowflake_sequence SET renew_time=now() WHERE node_id = ?`
)

var (
	// mu Solving the competition of the initialization of Sequence related library tables
	mu sync.Mutex

	finishInitTable = false

	workIdMax int64 = 1024
	_nodeId   string

	_defaultEpoch    int64 = 1533429240000
	_defaultNodeBits uint8 = 10
	_defaultStepBits uint8 = 12

	stepMask      int64 = -1 ^ (-1 << _defaultStepBits)
	timeShift           = _defaultNodeBits + _defaultStepBits
	workIdShift         = _defaultStepBits
	startWallTime       = time.Now()
)

type snowflakeSequence struct {
	mu sync.Mutex

	seqOffset  int8
	epoch      time.Time
	lastTime   int64
	step       int64
	workId     int64
	currentVal int64
}

// Start sequence and do some initialization operations
func (seq *snowflakeSequence) Start(ctx context.Context, conf proto.SequenceConfig) error {
	rt := ctx.Value(proto.RuntimeCtxKey{}).(runtime.Runtime)
	ctx = rcontext.WithRead(rcontext.WithDirect(ctx))

	if err := seq.initTableAndKeepalive(ctx, rt); err != nil {
		return err
	}

	// get work-id
	if err := seq.initWorkerID(ctx, rt, conf); err != nil {
		return err
	}

	curTime := startWallTime
	seq.epoch = curTime.Add(time.Unix(_defaultEpoch/1000, (_defaultEpoch%1000)*1000000).Sub(curTime))

	return nil
}

func (seq *snowflakeSequence) initTableAndKeepalive(ctx context.Context, rt runtime.Runtime) error {
	mu.Lock()
	defer mu.Unlock()

	if finishInitTable {
		return nil
	}

	tx, err := rt.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	ret, err := tx.Exec(ctx, "", _initTableSql)
	if err != nil {
		return err
	}
	_, _ = ret.RowsAffected()

	_nodeId = identity.GetNodeIdentity()

	if _, _, err := tx.Commit(ctx); err != nil {
		return err
	}

	k := &nodeKeepLive{rt: rt}
	go k.keepalive()

	finishInitTable = true
	return nil
}

func (seq *snowflakeSequence) initWorkerID(ctx context.Context, rt runtime.Runtime, conf proto.SequenceConfig) error {
	tx, err := rt.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	workId, err := seq.findWorkID(ctx, tx, conf.Name)
	if err != nil {
		return err
	}

	ret, err := tx.Exec(ctx, "", _setWorkId, proto.NewValueInt64(workId), proto.NewValueString(_nodeId), proto.NewValueString(conf.Name))
	if err != nil {
		return err
	}

	_, _ = ret.RowsAffected()

	if _, _, err := tx.Commit(ctx); err != nil {
		return err
	}

	seq.workId = workId

	if seq.workId < 0 || seq.workId > workIdMax {
		return fmt.Errorf("node worker-id must in [0, %d]", workIdMax)
	}
	return nil
}

// Acquire Apply for a self-increase ID
func (seq *snowflakeSequence) Acquire(ctx context.Context) (int64, error) {
	seq.mu.Lock()
	defer seq.mu.Unlock()

	timestamp := time.Since(seq.epoch).Nanoseconds() / 1000000

	if timestamp == seq.lastTime {
		seq.step = (seq.step + 1) & stepMask

		if seq.step == 0 {
			for timestamp <= seq.lastTime {
				timestamp = time.Since(seq.epoch).Nanoseconds() / 1000000
			}
		}
	} else {
		seq.seqOffset = ^seq.seqOffset & 1
		seq.step = int64(seq.seqOffset)
	}

	seq.lastTime = timestamp
	seq.currentVal = (timestamp)<<timeShift | (seq.workId << workIdShift) | (seq.step)

	return seq.currentVal, nil
}

func (seq *snowflakeSequence) Reset() error {
	return nil
}

func (seq *snowflakeSequence) Update() error {
	return nil
}

// Stop sequence
func (seq *snowflakeSequence) Stop() error {
	return nil
}

// CurrentVal get this sequence current val
func (seq *snowflakeSequence) CurrentVal() int64 {
	return atomic.LoadInt64(&seq.currentVal)
}

func (seq *snowflakeSequence) findWorkID(ctx context.Context, tx proto.Tx, seqName string) (int64, error) {
	ret, err := tx.Query(ctx, "", _selectSelfWorkIdWithXLock, proto.NewValueString(seqName), proto.NewValueString(_nodeId))
	if err != nil {
		return 0, err
	}
	ds, err := ret.Dataset()
	if err != nil {
		return 0, err
	}

	val := make([]proto.Value, 1)
	row, err := ds.Next()
	_, _ = ds.Next()
	if err == nil {
		if err := row.Scan(val); err != nil {
			return 0, err
		}

		if val[0] != nil {
			return val[0].Int64()
		}
	}

	ret, err = tx.Query(ctx, "", _selectMaxWorkIdWithXLock, proto.NewValueString(seqName))
	if err != nil {
		return 0, err
	}
	ds, err = ret.Dataset()
	if err != nil {
		return 0, err
	}

	row, err = ds.Next()
	if err != nil {
		return 0, err
	}
	_, _ = ds.Next()

	if err := row.Scan(val); err != nil {
		return 0, err
	}

	if val[0] == nil {
		return 1, nil
	}

	curId, _ := val[0].Int64()
	curId++
	if curId > workIdMax {
		ret, err := tx.Query(ctx, "", _selectFreeWorkIdWithXLock, proto.NewValueString(seqName))
		if err != nil {
			return 0, err
		}
		ret2, err := ret.Dataset()
		if err != nil {
			return 0, err
		}

		row, err := ret2.Next()
		if err != nil {
			return 0, err
		}
		_, _ = ret2.Next()

		if err := row.Scan(val); err != nil {
			return 0, err
		}

		curId, _ = val[0].Int64()
		curId++
	}

	return curId, nil
}

// nodeKeepLive do update renew time to keep work-id still belong to self
type nodeKeepLive struct {
	rt runtime.Runtime
}

func (n *nodeKeepLive) keepalive() {
	ticker := time.NewTicker(1 * time.Minute)

	f := func() {
		ctx := context.Background()

		tx, err := n.rt.Begin(ctx)
		if err != nil {
			log.Error("[Sequence][Snowflake] keepalive open tx fail", zap.String("node-id", _nodeId), zap.Error(err))
			return
		}

		defer tx.Rollback(ctx)

		ret, err := tx.Exec(context.Background(), "", _keepaliveNode, proto.NewValueString(_nodeId))
		if err != nil {
			log.Error("[Sequence][Snowflake] keepalive fail", zap.String("node-id", _nodeId), zap.Error(err))
			return
		}
		_, _ = ret.RowsAffected()

		if _, _, err := tx.Commit(ctx); err != nil {
			log.Error("[Sequence][Snowflake] keepalive tx commit fail", zap.String("node-id", _nodeId), zap.Error(err))
		}
	}

	for range ticker.C {
		f()
	}
}
