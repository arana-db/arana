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

import "github.com/prometheus/client_golang/prometheus"

var (
	ParserDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "parser",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in parse SQL.",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 25), //40us ~ 11min
	})

	OptimizeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "optimizer",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in optimizer.",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 25), //40us ~ 11min
	})

	ExecuteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "executor",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in execute.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 30), //100us ~ 15h,
	})
)

func RegisterMetrics() {
	prometheus.MustRegister(ParserDuration)
	prometheus.MustRegister(OptimizeDuration)
	prometheus.MustRegister(ExecuteDuration)
}
