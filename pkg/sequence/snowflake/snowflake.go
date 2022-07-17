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
	bsnowflake "github.com/bwmarrin/snowflake"

	"github.com/pkg/errors"

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

	//_setWorkId 插入一条新的 work_id
	_setWorkId = `REPLACE INTO __arana_snowflake_sequence(work_id, node_id, table_name, renew_time) VALUE (?, ?, ?, now())`

	//_selectSelfWorkIdWithXLock 查询下自己是否已经有一个 work-id 申请到了
	_selectSelfWorkIdWithXLock = `SELECT work_id FROM __arana_snowflake_sequence WHERE table_name = ? AND node_id = ? FOR UPDATE`

	//_selectMaxWorkIdWithXLock 选出当前最大的 work_id
	_selectMaxWorkIdWithXLock = `SELECT MAX(work_id) FROM __arana_snowflake_sequence WHERE table_name = ? FOR UPDATE`

	//_selectFreeWorkIdWithXLock 查询一个空闲的 workid，主要判断依据为 renew_time 超过 10 min 没有更新了，
	_selectFreeWorkIdWithXLock = `SELECT MAX(work_id) FROM __arana_snowflake_sequence WHERE table_name = ? AND renew_time < DATE_SUB(NOW(), INTERVAL 10 MINUTE) FOR UPDATE`

	//_updateWorkIdOwner 更新 workid 对应的 node_id 信息，进行 workid 复用
	_updateWorkIdOwner = `UPDATE __arana_snowflake_sequence SET node_id = ?, renew_time = now() WHERE table_name = ? AND work_id = ?`

	//_keepaliveNode 对 node 做续约保活动作
	_keepaliveNode = `UPDATE __arana_snowflake_sequence SET renew_time=now() WHERE node_id = ?`
)

var (
	// mu Solving the competition of the initialization of Sequence related library tables
	mu sync.Mutex

	finishInitTable = false

	workIdMax int64 = 1024
	_nodeId   string
)

type snowflakeSequence struct {
	mu sync.Mutex

	idGenerate *bsnowflake.Node
	epoch      time.Time
	lastTime   int64
	step       int64
	workdId    int64
	currentVal int64
}

// Start sequence and do some initialization operations
func (seq *snowflakeSequence) Start(ctx context.Context, conf proto.SequenceConfig) error {
	schema := rcontext.Schema(ctx)
	rt, err := runtime.Load(schema)
	if err != nil {
		return errors.Wrapf(err, "snowflake: no runtime found for namespace '%s'", schema)
	}

	ctx = rcontext.WithRead(rcontext.WithDirect(ctx))

	if err := seq.initTableAndKeepalive(ctx, rt); err != nil {
		return err
	}

	// get work-id
	if err := seq.initWorkerID(ctx, rt, conf); err != nil {
		return errors.WithStack(err)
	}

	node, err := bsnowflake.NewNode(seq.workdId)
	if err != nil {
		return err
	}

	seq.idGenerate = node

	return nil
}

func (seq *snowflakeSequence) initTableAndKeepalive(ctx context.Context, rt runtime.Runtime) error {
	mu.Lock()
	defer mu.Unlock()

	if finishInitTable {
		return nil
	}

	if _, err := rt.Exec(ctx, "", _initTableSql); err != nil {
		return err
	}

	nodeId, err := identity.GetNodeIdentity()
	if err != nil {
		return err
	}
	_nodeId = nodeId

	k := &nodeKeepLive{rt: rt}
	go k.keepalive()

	finishInitTable = true
	return nil
}

func (seq *snowflakeSequence) initWorkerID(ctx context.Context, rt runtime.Runtime, conf proto.SequenceConfig) error {
	tx, err := rt.Begin(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	defer tx.Rollback(ctx)

	workId, err := seq.findWorkID(ctx, tx, conf.Name)
	if err != nil {
		return errors.WithStack(err)
	}

	ret, err := tx.Exec(ctx, "", _setWorkId, workId, _nodeId, conf.Name)
	if err != nil {
		return errors.WithStack(err)
	}

	_, _ = ret.RowsAffected()

	if _, _, err := tx.Commit(ctx); err != nil {
		return errors.WithStack(err)
	}

	seq.workdId = workId

	if seq.workdId < 0 || seq.workdId > workIdMax {
		return fmt.Errorf("node worker-id must in [0, %d]", workIdMax)
	}
	return nil
}

// Acquire Apply for a self-increase ID
func (seq *snowflakeSequence) Acquire(ctx context.Context) (int64, error) {

	id := seq.idGenerate.Generate()

	//无锁更新当前 sequence 的 id 信息
	for {
		cur := seq.CurrentVal()
		if id.Int64() > cur {
			if atomic.CompareAndSwapInt64(&seq.currentVal, cur, id.Int64()) {
				break
			}
		}
	}

	return id.Int64(), nil
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
	ret, err := tx.Query(ctx, "", _selectSelfWorkIdWithXLock, seqName, _nodeId)
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
			return val[0].(int64), nil
		}
	}

	ret, err = tx.Query(ctx, "", _selectMaxWorkIdWithXLock, seqName)
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

	curId := val[0].(int64) + 1
	if curId > workIdMax {
		ret, err := tx.Query(ctx, "", _selectFreeWorkIdWithXLock, seqName)
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

		curId = val[0].(int64) + 1
	}

	return curId, nil
}

// nodeKeepLive do update renew time to keep work-id still belong to self
type nodeKeepLive struct {
	rt runtime.Runtime
}

func (n *nodeKeepLive) keepalive() {
	ticker := time.NewTicker(1 * time.Minute)

	for range ticker.C {
		_, err := n.rt.Exec(context.Background(), "", _keepaliveNode, _nodeId)

		if err != nil {
			log.Error("[Sequence][Snowflake] keepalive fail", zap.String("node-id", _nodeId), zap.Error(err))
		}
	}
}
