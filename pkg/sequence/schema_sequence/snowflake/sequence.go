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
	"time"

	"github.com/arana-db/arana/pkg/proto"
)

func init() {
	proto.RegisterSequence(_sequencePluginName, func() proto.EnchanceSequence {
		return &snowflakeSequence{}
	})
}

const (
	_sequencePluginName = "snowflake"
)

const (
	_initTableSql = `
	CREATE TABLE IF NOT EXISTS __arana_snowflake_sequence(
	  work_id int auto_increment '主键',
	  node_id varchar(255) not null '节点的唯一标识',
	  table_name varchar(255) not null 'arana中的逻辑表名称',
	  primary key (work_id)
	) ENGINE = InnoDB;
	`

	_getWorkId = `
INSERT IGNORE INTO __arana_snowflake_sequence(node_id, table_name) VALUE (?, ?)
	`
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

// Start 启动 Sequence，做一些初始化操作
func (seq *snowflakeSequence) Start(ctx context.Context, conf proto.SequenceConfig) error {
	return nil
}

func (seq *snowflakeSequence) doInit(ctx context.Context, conf proto.SequenceConfig) error {
	// get work-id
	err := func() error {
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

		vconn.Exec(ctx, "", _getWorkId, "", conf.Name)

		return nil
	}()

	if err != nil {
		return err
	}

	if seq.workdId < 0 || seq.workdId > workIdMax {
		return fmt.Errorf("node worker-id must in [0, %d]", workIdMax)
	}

	curTime := startWallTime
	seq.epoch = curTime.Add(time.Unix(_defaultEpoch/1000, (_defaultEpoch%1000)*1000000).Sub(curTime))

	return nil
}

// Acquire 申请一个自增ID
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

// Stop 停止该 Sequence 的工作
func (seq *snowflakeSequence) Stop() error {
	return nil
}
