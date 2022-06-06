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
	"sync/atomic"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func init() {
	proto.RegisterSequence(SequencePluginName, func() proto.EnchanceSequence {
		return &groupSequence{}
	})
}

const (
	SequencePluginName = "group"
)

type groupSequence struct {
	workdId      int32
	curentVal    int64
	preTimestamp int64
}

// Start 启动 Sequence，做一些初始化操作
func (seq *groupSequence) Start(ctx context.Context, option proto.SequenceConfig) error {
	return nil
}

// Acquire 申请一个自增ID
func (seq *groupSequence) Acquire(ctx context.Context) (int64, error) {
	return 0, nil
}

func (seq *groupSequence) Reset() error {
	return nil
}

func (seq *groupSequence) Update() error {
	return nil
}

// Stop 停止该 Sequence 的工作
func (seq *groupSequence) Stop() error {
	return nil
}

func (seq *groupSequence) CurrentVal() int64 {
	return atomic.LoadInt64(&seq.curentVal)
}
