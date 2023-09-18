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

package trace

import (
	"context"
	"sync"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
)

type TestProvider struct{}

func (t *TestProvider) Initialize(_ context.Context, traceCfg *config.Trace) error {
	return nil
}

func (t *TestProvider) Extract(ctx *proto.Context, hints []*hint.Hint) bool {
	return false
}

func TestUseTraceProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tCtx := context.Background()
	tPCtx := &proto.Context{}
	tProviderType := "test_provider"
	tTraceCfg := &config.Trace{
		Type:    tProviderType,
		Address: "test_address",
	}

	tTraceCfgF := &config.Trace{
		Type:    "test_provider_2",
		Address: "test_address",
	}

	// test for Initialize fail without Register
	err := Initialize(tCtx, tTraceCfgF)
	assert.Error(t, err)
	once = sync.Once{}

	// test for Register ability
	tProvider := &TestProvider{}
	RegisterProviders(ProviderType(tProviderType), tProvider)
	assert.Equal(t, tProvider, providers[ProviderType(tProviderType)])

	// test for Initialize ability
	err = Initialize(tCtx, tTraceCfg)
	assert.NoError(t, err)

	// test for Extract ability
	hasExt := Extract(tPCtx, nil)
	assert.Equal(t, false, hasExt)
}
