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
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const SequencePluginName = "group"

func init() {
	proto.RegisterSequence(SequencePluginName, func() proto.EnhancedSequence {
		return &groupSequence{}
	})
}

type groupSequence struct {
	workerId     int32
	currentVal   int64
	preTimestamp int64
}

// Start Starts sequence and do some initialization operations
func (seq *groupSequence) Start(ctx context.Context, option proto.SequenceConfig) error {
	return nil
}

// Acquire Apply for a increase ID
func (seq *groupSequence) Acquire(ctx context.Context) (int64, error) {
	return 0, nil
}

// Reset reset sequence info
func (seq *groupSequence) Reset() error {
	return nil
}

// Update update sequence info
func (seq *groupSequence) Update() error {
	return nil
}

// Stop stops sequence
func (seq *groupSequence) Stop() error {
	return nil
}

// CurrentVal get this sequence current val
func (seq *groupSequence) CurrentVal() int64 {
	return seq.currentVal
}
