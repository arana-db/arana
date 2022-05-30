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
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	ErrorNotSequenceType  error = errors.New("sequence type not found")
	ErrorNotFoundSequence error = errors.New("sequence instance not found")
)

func init() {
	proto.SetSequenceManagerCreator(func() proto.SequenceManager {
		return &sequenceManager{
			sequenceOptions:  make(map[string]proto.SequenceConfig),
			sequenceRegistry: make(map[string]proto.EnchanceSequence),
		}
	})
}

// SequenceManager 统一管理 Seqneuce 的 Manager
type sequenceManager struct {
	lock             sync.RWMutex
	sequenceOptions  map[string]proto.SequenceConfig
	sequenceRegistry map[string]proto.EnchanceSequence
}

// CreateSequence
func (sMgn *sequenceManager) CreateSequence(ctx context.Context, conn proto.VConn, conf proto.SequenceConfig) (proto.Sequence, error) {
	sMgn.lock.RLock()
	if seq, exist := sMgn.sequenceRegistry[conf.Name]; exist {
		sMgn.lock.RUnlock()
		return seq, nil
	}

	sMgn.lock.RUnlock()

	seqType := conf.Type

	builder, ok := proto.GetSequenceSupplier(seqType)
	if !ok {
		log.Errorf("sequence=[%s] not exist", seqType)
		return nil, ErrorNotSequenceType
	}

	ctx = context.WithValue(ctx, proto.ContextVconnKey, conn)

	sequence := builder()
	if err := sequence.Start(ctx, conf); err != nil {
		return nil, err
	}

	sMgn.lock.Lock()
	defer sMgn.lock.Unlock()

	sMgn.sequenceOptions[conf.Name] = conf
	sMgn.sequenceRegistry[conf.Name] = sequence

	return sequence, nil
}

// GetSequence
func (sMgn *sequenceManager) GetSequence(ctx context.Context, table string) (proto.Sequence, error) {
	sMgn.lock.RLock()
	defer sMgn.lock.RUnlock()

	seq, ok := sMgn.sequenceRegistry[table]

	if !ok {
		log.Errorf("sequence=[%s] not found", table)
		return nil, ErrorNotFoundSequence
	}

	return seq, nil
}

func BuildAutoIncrementName(table string) string {
	return fmt.Sprintf("__arana_incr_%s", table)
}
