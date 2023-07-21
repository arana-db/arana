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
	"sync"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
)

func init() {
	proto.RegisterSequenceManager(newSequenceManager())
}

func newSequenceManager() proto.SequenceManager {
	return &sequenceManager{
		tenants: map[string]*tenantBucket{},
	}
}

// SequenceManager uniform management of sequence manager
type sequenceManager struct {
	lock    sync.RWMutex
	tenants map[string]*tenantBucket
}

type tenantBucket struct {
	lock    sync.RWMutex
	schemas map[string]*schemaBucket
}

func (t *tenantBucket) getSchema(schema string) *schemaBucket {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.schemas[schema]
}

func (t *tenantBucket) getOrCreate(schema string) *schemaBucket {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.schemas[schema]; !ok {
		t.schemas[schema] = &schemaBucket{
			sequenceRegistry: map[string]proto.EnhancedSequence{},
		}
	}

	return t.schemas[schema]
}

type schemaBucket struct {
	lock             sync.RWMutex
	sequenceRegistry map[string]proto.EnhancedSequence
}

func (t *schemaBucket) getSequence(name string) (proto.Sequence, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	val, ok := t.sequenceRegistry[name]
	if !ok {
		return nil, proto.ErrorNotFoundSequence
	}

	return val, nil
}

func (t *schemaBucket) createIfAbsent(name string, f func() (proto.EnhancedSequence, error)) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, ok := t.sequenceRegistry[name]
	if !ok {
		val, err := f()
		if err != nil {
			return err
		}

		t.sequenceRegistry[name] = val
	}

	return nil
}

// CreateSequence creates one sequence instance
func (m *sequenceManager) CreateSequence(ctx context.Context, tenant, schema string, conf proto.SequenceConfig) (proto.Sequence, error) {
	m.lock.Lock()
	if _, ok := m.tenants[tenant]; !ok {
		m.tenants[tenant] = &tenantBucket{
			schemas: map[string]*schemaBucket{},
		}
	}

	tbucket := m.tenants[tenant]
	m.lock.Unlock()

	sbucket := tbucket.getOrCreate(schema)
	if val, _ := sbucket.getSequence(conf.Name); val != nil {
		return val, nil
	}

	builder, ok := proto.GetSequenceSupplier(conf.Type)
	if !ok {
		log.Errorf("[sequence] name=%s not exist", conf.Type)
		return nil, proto.ErrorNotSequenceType
	}

	if err := sbucket.createIfAbsent(conf.Name, func() (proto.EnhancedSequence, error) {
		rt, err := runtime.Load(rcontext.Tenant(ctx), schema)
		if err != nil {
			log.Errorf("[sequence] load runtime.Runtime from schema=%s fail, %s", schema, err.Error())
			return nil, err
		}

		sequence := builder()

		ctx := context.WithValue(ctx, proto.RuntimeCtxKey{}, rt)

		if err := sequence.Start(ctx, conf); err != nil {
			log.Errorf("[sequence] type=%s name=%s start fail, %s", conf.Type, conf.Name, err.Error())
			return nil, err
		}

		return sequence, nil
	}); err != nil {
		return nil, err
	}

	return sbucket.getSequence(conf.Name)
}

// GetSequence gets sequence instance by name
func (m *sequenceManager) GetSequence(ctx context.Context, tenant, schema, name string) (proto.Sequence, error) {
	m.lock.RLock()
	tbucket, ok := m.tenants[tenant]
	m.lock.RUnlock()

	if !ok {
		return nil, proto.ErrorNotFoundSequence
	}

	sbucket := tbucket.getSchema(schema)
	if sbucket == nil {
		return nil, proto.ErrorNotFoundSequence
	}
	return sbucket.getSequence(name)
}
