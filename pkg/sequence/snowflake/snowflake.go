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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/identity"
)

func init() {
	proto.RegisterSequence(SequencePluginName, func() proto.EnchanceSequence {
		return &snowflakeSequence{}
	})
}

const (
	SequencePluginName = "snowflake"
)

const (
	_initTableSql = `
	CREATE TABLE IF NOT EXISTS __arana_snowflake_sequence (
		work_id int AUTO_INCREMENT COMMENT '主键',
		node_id varchar(255) NOT NULL COMMENT '节点的唯一标识',
		table_name varchar(255) NOT NULL COMMENT 'arana中的逻辑表名称',
		PRIMARY KEY (work_id),
		UNIQUE KEY(node_id, table_name)
	) ENGINE = InnoDB;
	`

	_getWorkId = `INSERT IGNORE INTO __arana_snowflake_sequence(node_id, table_name) VALUE (?, ?)`
)

var (
	mu sync.Mutex

	finishInitTable bool = false

	_defaultEpoch    int64 = 1533429240000
	_defaultNodeBits uint8 = 10
	_defaultStepBits uint8 = 12

	workIdMax     int64 = 1024
	stepMask      int64 = -1 ^ (-1 << _defaultStepBits)
	timeShift           = _defaultNodeBits + _defaultNodeBits
	workIdShift         = _defaultStepBits
	startWallTime       = time.Now()
)

type snowflakeSequence struct {
	mu sync.Mutex

	epoch     time.Time
	lastTime  int64
	step      int64
	workdId   int64
	curentVal int64
}

// Start Start sequence and do some initialization operations
func (seq *snowflakeSequence) Start(ctx context.Context, conf proto.SequenceConfig) error {
	if err := seq.doInit(ctx, conf); err != nil {
		return err
	}

	return nil
}

func (seq *snowflakeSequence) doInit(ctx context.Context, conf proto.SequenceConfig) error {
	// get work-id
	if err := func() error {
		mu.Lock()
		defer mu.Unlock()

		vconn, ok := ctx.Value(proto.ContextVconnKey).(proto.VConn)
		if !ok {
			return errors.New("snowflake init need proto.VConn")
		}
		if !finishInitTable {
			if _, err := vconn.Exec(ctx, "", _initTableSql); err != nil {
				return err
			}
		}
		finishInitTable = true

		if _, err := vconn.Exec(ctx, "", _getWorkId, identity.GetNodeIdentity(), conf.Name); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		return err
	}

	if seq.workdId < 0 || seq.workdId > workIdMax {
		return fmt.Errorf("node worker-id must in [0, %d]", workIdMax)
	}

	curTime := startWallTime
	seq.epoch = curTime.Add(time.Unix(_defaultEpoch/1000, (_defaultEpoch%1000)*1000000).Sub(curTime))

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
		seq.step = 0
	}

	seq.lastTime = timestamp
	seq.curentVal = int64((timestamp)<<timeShift | (seq.workdId << workIdShift) | (seq.step))

	return seq.curentVal, nil
}

func (seq *snowflakeSequence) Reset() error {
	return nil
}

func (seq *snowflakeSequence) Update() error {
	return nil
}

// Stop 停止该 Sequence 的工作
func (seq *snowflakeSequence) Stop() error {
	return nil
}

func (seq *snowflakeSequence) CurrentVal() int64 {
	return atomic.LoadInt64(&seq.curentVal)
}
