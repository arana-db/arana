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
 *
 */

package boot

import (
	"context"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

func (fp *discovery) WatchTenants(ctx context.Context) (<-chan config.TenantsEvent, context.CancelFunc, error) {
	ch := make(chan config.TenantsEvent)

	cancel := fp.tenantOp.Subscribe(ctx, func(e config.Event) {
		ch <- e.(config.TenantsEvent)
	})

	return ch, wrapWatchCancel(cancel, func() {
		close(ch)
	}), nil
}

func (fp *discovery) WatchNodes(ctx context.Context, tenant string) (<-chan config.NodesEvent, context.CancelFunc, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, nil, ErrorNoTenant
	}

	ch := make(chan config.NodesEvent)

	cancel := op.Subscribe(ctx, config.EventTypeNodes, func(e config.Event) {
		ch <- e.(config.NodesEvent)
	})

	return ch, wrapWatchCancel(cancel, func() {
		close(ch)
	}), nil
}

func (fp *discovery) WatchUsers(ctx context.Context, tenant string) (<-chan config.UsersEvent, context.CancelFunc, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, nil, ErrorNoTenant
	}

	ch := make(chan config.UsersEvent)

	cancel := op.Subscribe(ctx, config.EventTypeUsers, func(e config.Event) {
		ch <- e.(config.UsersEvent)
	})

	return ch, wrapWatchCancel(cancel, func() {
		close(ch)
	}), nil
}

func (fp *discovery) WatchClusters(ctx context.Context, tenant string) (<-chan config.ClustersEvent, context.CancelFunc, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, nil, ErrorNoTenant
	}

	ch := make(chan config.ClustersEvent)

	cancel := op.Subscribe(ctx, config.EventTypeClusters, func(e config.Event) {
		ch <- e.(config.ClustersEvent)
	})

	return ch, wrapWatchCancel(cancel, func() {
		close(ch)
	}), nil
}

func (fp *discovery) WatchShardingRule(ctx context.Context, tenant string) (<-chan config.ShardingRuleEvent, context.CancelFunc, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, nil, ErrorNoTenant
	}

	ch := make(chan config.ShardingRuleEvent)

	cancel := op.Subscribe(ctx, config.EventTypeShardingRule, func(e config.Event) {
		ch <- e.(config.ShardingRuleEvent)
	})

	return ch, wrapWatchCancel(cancel, func() {
		close(ch)
	}), nil
}

func (fp *discovery) WatchShadowRule(ctx context.Context, tenant string) (<-chan config.ShadowRuleEvent, context.CancelFunc, error) {
	op, ok := fp.centers[tenant]
	if !ok {
		return nil, nil, ErrorNoTenant
	}

	ch := make(chan config.ShadowRuleEvent)

	cancel := op.Subscribe(ctx, config.EventTypeShadowRule, func(e config.Event) {
		ch <- e.(config.ShadowRuleEvent)
	})

	return ch, wrapWatchCancel(cancel, func() {
		close(ch)
	}), nil
}

func wrapWatchCancel(cancel context.CancelFunc, closeChan func()) context.CancelFunc {
	return func() {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		cancel()
		<-timer.C
		closeChan()
	}
}
