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

package metrics

import (
	"context"
	"strings"
	"testing"
	"time"
)

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

import (
	"github.com/arana-db/arana/third_party/pools"
)

type TestResource struct {
	num    int64
	closed bool
}

func (tr *TestResource) Close() {
	tr.closed = true
}

func PoolFactory(ctx context.Context) (pools.Resource, error) {
	return &TestResource{num: 1, closed: false}, nil
}

func TestResourcePoolCollector_Collect(t *testing.T) {
	waitFunc := func(start time.Time) {
		// do nothing
	}
	pool := pools.NewResourcePool(PoolFactory, 10, 10, 1000, 0, waitFunc)

	collector := NewResourcePoolCollector(pool)
	prometheus.MustRegister(collector)

	expected := `
		# HELP resource_pool_capacity Capacity of the resource pool
		# TYPE resource_pool_capacity gauge
		resource_pool_capacity 10
		# HELP resource_pool_available Available resources in the pool
		# TYPE resource_pool_available gauge
		resource_pool_available 10
		# HELP resource_pool_active Active resources in the pool
		# TYPE resource_pool_active gauge
		resource_pool_active 0
		# HELP resource_pool_in_use Resources in use in the pool
		# TYPE resource_pool_in_use gauge
		resource_pool_in_use 0
		# HELP resource_pool_wait_count Number of waits for resources
		# TYPE resource_pool_wait_count counter
		resource_pool_wait_count 0
		# HELP resource_pool_wait_time Total wait time for resources
		# TYPE resource_pool_wait_time counter
		resource_pool_wait_time 0
		# HELP resource_pool_idle_timeout Timeout for idle resources
		# TYPE resource_pool_idle_timeout gauge
		resource_pool_idle_timeout 1000
		# HELP resource_pool_idle_closed Number of idle resources closed due to timeout
		# TYPE resource_pool_idle_closed counter
		resource_pool_idle_closed 0
		# HELP resource_pool_exhausted Number of times the pool was exhausted
		# TYPE resource_pool_exhausted counter
		resource_pool_exhausted 0
	`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(expected)); err != nil {
		t.Fatal(err)
	}
}
