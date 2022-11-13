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

package group

import (
	"context"
	"io"
	"strconv"
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
)

func init() {
	proto.RegisterSequence(SequencePluginName, func() proto.EnhancedSequence {
		return &groupSequence{}
	})
}

const (
	SequencePluginName = "group"

	_stepKey                = "step"
	_startSequence    int64 = 1
	_defaultGroupStep int64 = 100

	_initGroupSequenceTableSql = `
	CREATE TABLE IF NOT EXISTS __arana_group_sequence (
		id          int AUTO_INCREMENT COMMENT 'primary key',
		seq_val     bigint COMMENT 'the current group start value',
		step        int COMMENT 'the step of the group',
		table_name  varchar(255) NOT NULL COMMENT 'arana logic table name',
		renew_time  datetime NOT NULL COMMENT 'node renew time',
		PRIMARY KEY(id),
		UNIQUE KEY(table_name)
	) ENGINE = InnoDB;
	`
	_initGroupSequence        = `INSERT INTO __arana_group_sequence(seq_val, step, table_name, renew_time) VALUE (?, ?, ?, now())`
	_selectNextGroupWithXLock = `SELECT seq_val FROM __arana_group_sequence WHERE table_name = ? FOR UPDATE`
	_updateNextGroup          = `UPDATE __arana_group_sequence set seq_val = ?, renew_time = now() WHERE table_name = ?`
)

var (
	// mu Solving the competition of the initialization of Sequence related library tables
	mu sync.Mutex

	finishInitTable = false
)

type groupSequence struct {
	mu sync.Mutex

	tableName string
	step      int64

	nextGroupStartVal  int64
	nextGroupMaxVal    int64
	currentGroupMaxVal int64
	currentVal         int64
}

// Start sequence and do some initialization operations
func (seq *groupSequence) Start(ctx context.Context, option proto.SequenceConfig) error {
	rt := ctx.Value(proto.RuntimeCtxKey{}).(runtime.Runtime)
	ctx = rcontext.WithRead(rcontext.WithDirect(ctx))

	// init table
	if err := seq.initTable(ctx, rt); err != nil {
		return err
	}

	// init sequence
	if err := seq.initStep(option); err != nil {
		return err
	}

	seq.tableName = option.Name
	return nil
}

func (seq *groupSequence) initTable(ctx context.Context, rt runtime.Runtime) error {
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

	ret, err := tx.Exec(ctx, "", _initGroupSequenceTableSql)
	if err != nil {
		return err
	}
	_, _ = ret.RowsAffected()

	if _, _, err := tx.Commit(ctx); err != nil {
		return err
	}

	finishInitTable = true
	return nil
}

func (seq *groupSequence) initStep(option proto.SequenceConfig) error {
	seq.mu.Lock()
	defer seq.mu.Unlock()

	var step int64
	stepValue, ok := option.Option[_stepKey]
	if ok {
		tempStep, err := strconv.Atoi(stepValue)
		if err != nil {
			return err
		}
		step = int64(tempStep)
	} else {
		step = _defaultGroupStep
	}
	seq.step = step

	return nil
}

// Acquire Apply for an increase ID
func (seq *groupSequence) Acquire(ctx context.Context) (int64, error) {
	seq.mu.Lock()
	defer seq.mu.Unlock()

	if seq.currentVal >= seq.currentGroupMaxVal {
		schema := rcontext.Schema(ctx)
		rt, err := runtime.Load(schema)
		if err != nil {
			log.Errorf("[sequence] load runtime.Runtime from schema=%s fail, %s", schema, err.Error())
			return 0, err
		}

		err = seq.acquireNextGroup(ctx, rt)
		if err != nil {
			return 0, err
		}
		seq.currentVal = seq.nextGroupStartVal
		seq.currentGroupMaxVal = seq.nextGroupMaxVal
	} else {
		seq.currentVal++
	}

	return seq.currentVal, nil
}

func (seq *groupSequence) acquireNextGroup(ctx context.Context, rt runtime.Runtime) error {
	ctx = rcontext.WithDirect(ctx)
	tx, err := rt.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	rs, err := tx.Query(ctx, "", _selectNextGroupWithXLock, seq.tableName)
	if err != nil {
		return err
	}

	ds, err := rs.Dataset()
	if err != nil {
		return err
	}
	val := make([]proto.Value, 1)
	row, err := ds.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			seq.nextGroupStartVal = _startSequence
			seq.nextGroupMaxVal = _startSequence + seq.step - 1
			rs, err := tx.Exec(ctx, "", _initGroupSequence, seq.nextGroupMaxVal+1, seq.step, seq.tableName)
			if err != nil {
				return err
			}
			_, _ = rs.RowsAffected()

			_, _, err = tx.Commit(ctx)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	if err = row.Scan(val); err != nil {
		return err
	}
	_, _ = ds.Next()

	if val[0] != nil {
		nextGroupStartVal := val[0].(int64)
		if nextGroupStartVal%seq.step != 0 {
			// padding left
			nextGroupStartVal = (nextGroupStartVal/seq.step + 1) * seq.step
		}
		seq.nextGroupStartVal = nextGroupStartVal
		seq.nextGroupMaxVal = seq.nextGroupStartVal + seq.step - 1
		rs, err := tx.Exec(ctx, "", _updateNextGroup, seq.nextGroupMaxVal+1, seq.tableName)
		if err != nil {
			return err
		}
		_, _ = rs.RowsAffected()

		_, _, err = tx.Commit(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Reset resets sequence info
func (seq *groupSequence) Reset() error {
	return nil
}

// Update updates sequence info
func (seq *groupSequence) Update() error {
	return nil
}

// Stop stops sequence
func (seq *groupSequence) Stop() error {
	return nil
}

// CurrentVal gets this sequence current val
func (seq *groupSequence) CurrentVal() int64 {
	return seq.currentVal
}
