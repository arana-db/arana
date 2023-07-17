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
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
)

const (
	Service              = "arana"
	Jaeger  ProviderType = "jaeger"
)

type ProviderType string

var (
	providers       = make(map[ProviderType]Provider, 8)
	currentProvider Provider
	once            sync.Once
)

func RegisterProviders(pType ProviderType, p Provider) {
	providers[pType] = p
}

func Initialize(ctx context.Context, traceCfg *config.Trace) error {
	var err error
	once.Do(func() {
		v, ok := providers[ProviderType(traceCfg.Type)]
		if !ok {
			err = errors.Errorf("not supported %s trace provider", traceCfg.Type)
			return
		}
		currentProvider = v
		err = currentProvider.Initialize(ctx, traceCfg)
	})
	return err
}

func Extract(ctx *proto.Context, hints []*hint.Hint) bool {
	return currentProvider.Extract(ctx, hints)
}

type Provider interface {
	Initialize(ctx context.Context, traceCfg *config.Trace) error
	Extract(ctx *proto.Context, hints []*hint.Hint) bool
}
