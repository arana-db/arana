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
	"github.com/arana-db/arana/third_party/pools"
	"github.com/prometheus/client_golang/prometheus"
)

type resourcePoolCollector struct {
	pool            *pools.ResourcePool
	capacityDesc    *prometheus.Desc
	availableDesc   *prometheus.Desc
	activeDesc      *prometheus.Desc
	inUseDesc       *prometheus.Desc
	waitCountDesc   *prometheus.Desc
	waitTimeDesc    *prometheus.Desc
	idleTimeoutDesc *prometheus.Desc
	idleClosedDesc  *prometheus.Desc
	exhaustedDesc   *prometheus.Desc
}

func NewResourcePoolCollector(pool *pools.ResourcePool) *resourcePoolCollector {
	return &resourcePoolCollector{
		pool:            pool,
		capacityDesc:    prometheus.NewDesc("resource_pool_capacity", "Capacity of the resource pool", nil, nil),
		availableDesc:   prometheus.NewDesc("resource_pool_available", "Available resources in the pool", nil, nil),
		activeDesc:      prometheus.NewDesc("resource_pool_active", "Active resources in the pool", nil, nil),
		inUseDesc:       prometheus.NewDesc("resource_pool_in_use", "Resources in use in the pool", nil, nil),
		waitCountDesc:   prometheus.NewDesc("resource_pool_wait_count", "Number of waits for resources", nil, nil),
		waitTimeDesc:    prometheus.NewDesc("resource_pool_wait_time", "Total wait time for resources", nil, nil),
		idleTimeoutDesc: prometheus.NewDesc("resource_pool_idle_timeout", "Timeout for idle resources", nil, nil),
		idleClosedDesc:  prometheus.NewDesc("resource_pool_idle_closed", "Number of idle resources closed due to timeout", nil, nil),
		exhaustedDesc:   prometheus.NewDesc("resource_pool_exhausted", "Number of times the pool was exhausted", nil, nil),
	}
}

func (c *resourcePoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.capacityDesc
	ch <- c.availableDesc
	ch <- c.activeDesc
	ch <- c.inUseDesc
	ch <- c.waitCountDesc
	ch <- c.waitTimeDesc
	ch <- c.idleTimeoutDesc
	ch <- c.idleClosedDesc
	ch <- c.exhaustedDesc
}

func (c *resourcePoolCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.capacityDesc, prometheus.GaugeValue, float64(c.pool.Capacity()))
	ch <- prometheus.MustNewConstMetric(c.availableDesc, prometheus.GaugeValue, float64(c.pool.Available()))
	ch <- prometheus.MustNewConstMetric(c.activeDesc, prometheus.GaugeValue, float64(c.pool.Active()))
	ch <- prometheus.MustNewConstMetric(c.inUseDesc, prometheus.GaugeValue, float64(c.pool.InUse()))
	ch <- prometheus.MustNewConstMetric(c.waitCountDesc, prometheus.CounterValue, float64(c.pool.WaitCount()))
	ch <- prometheus.MustNewConstMetric(c.waitTimeDesc, prometheus.CounterValue, float64(c.pool.WaitTime()))
	ch <- prometheus.MustNewConstMetric(c.idleTimeoutDesc, prometheus.GaugeValue, float64(c.pool.IdleTimeout()))
	ch <- prometheus.MustNewConstMetric(c.idleClosedDesc, prometheus.CounterValue, float64(c.pool.IdleClosed()))
	ch <- prometheus.MustNewConstMetric(c.exhaustedDesc, prometheus.CounterValue, float64(c.pool.Exhausted()))
}
