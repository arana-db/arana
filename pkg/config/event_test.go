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

package config

import (
	"testing"
)

func TestEventTypes(t *testing.T) {
	tests := []struct {
		name string
		e    Event
		want EventType
	}{
		{
			name: "Test TenantsEvent type",
			e:    TenantsEvent{},
			want: EventTypeTenants,
		},
		{
			name: "Test NodesEvent type",
			e:    NodesEvent{},
			want: EventTypeNodes,
		},
		{
			name: "Test UsersEvent type",
			e:    UsersEvent{},
			want: EventTypeUsers,
		},
		{
			name: "Test ClustersEvent type",
			e:    ClustersEvent{},
			want: EventTypeClusters,
		},
		{
			name: "Test ShardingRuleEvent type",
			e:    ShardingRuleEvent{},
			want: EventTypeShardingRule,
		},
		{
			name: "Test ShadowRuleEvent type",
			e:    ShadowRuleEvent{},
			want: EventTypeShadowRule,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Type(); got != tt.want {
				t.Errorf("Event.Type() = %v, want %v", got, tt.want)
			}
		})
	}
}
