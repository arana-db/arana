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

package runtime

import (
	"sync"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/runtime/namespace"
)

func TestLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const schemaName = "FakeSchema"

	rt, err := Load(schemaName)
	assert.Error(t, err)
	assert.Nil(t, rt)
	ns, err := namespace.New(schemaName)
	assert.NoError(t, err)
	_ = namespace.Register(ns)
	defer func() {
		_ = namespace.Unregister(schemaName)
	}()

	rt, err = Load(schemaName)
	assert.NoError(t, err)
	assert.NotNil(t, rt)
}

func TestNextTxID(t *testing.T) {
	const total = 1000
	var (
		m  sync.Map
		wg sync.WaitGroup
	)
	wg.Add(total)
	for range [total]struct{}{} {
		go func() {
			defer wg.Done()
			_, loaded := m.LoadOrStore(nextTxID(), struct{}{})
			assert.False(t, loaded)
		}()
	}

	wg.Wait()
}
