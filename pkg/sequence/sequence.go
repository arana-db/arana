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

package sequence

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

import (
	"go.uber.org/zap"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	ErrorNotSequenceType  error = errors.New("sequence type not found")
	ErrorNotFoundSequence error = errors.New("sequence instance not found")
)

func NewSequenceManager() proto.SequenceManager {
	return &sequenceManager{
		sequenceOptions:  make(map[string]proto.SequenceConfig),
		sequenceRegistry: make(map[string]proto.EnchanceSequence),
	}
}

// SequenceManager Uniform management of seqneuce manager
type sequenceManager struct {
	lock             sync.RWMutex
	sequenceOptions  map[string]proto.SequenceConfig
	sequenceRegistry map[string]proto.EnchanceSequence
}

// CreateSequence create one sequence instance
func (m *sequenceManager) CreateSequence(ctx context.Context, conn proto.VConn, conf proto.SequenceConfig) (proto.Sequence, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if seq, exist := m.sequenceRegistry[conf.Name]; exist {
		return seq, nil
	}

	seqType := conf.Type

	builder, ok := proto.GetSequenceSupplier(seqType)
	if !ok {
		log.Errorf("sequence=[%s] not exist", seqType)
		return nil, ErrorNotSequenceType
	}

	ctx = context.WithValue(ctx, proto.VConnCtxKey{}, conn)

	sequence := builder()
	if err := sequence.Start(ctx, conf); err != nil {
		return nil, err
	}

	m.sequenceOptions[conf.Name] = conf
	m.sequenceRegistry[conf.Name] = sequence

	return sequence, nil
}

// GetSequence get sequence instance by name
func (m *sequenceManager) GetSequence(ctx context.Context, name string) (proto.Sequence, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	seq, ok := m.sequenceRegistry[name]

	if !ok {
		log.Warn("sequence not found", zap.String("name", name))
		return nil, ErrorNotFoundSequence
	}

	return seq, nil
}

func BuildAutoIncrementName(table string) string {
	return fmt.Sprintf("__arana_incr_%s", table)
}
