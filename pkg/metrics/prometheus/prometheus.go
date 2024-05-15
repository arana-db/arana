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

package prometheus

import (
	"context"

	"fmt"
	"net/http"

	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/metrics"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Prometheus struct {
	*MetricsManager
}

func init() {
	metrics.RegisterProviders(metrics.Prometheus, &Prometheus{})
}

func (p *Prometheus) Initialize(ctx context.Context, metricCfg *config.Metric) error {
	var err error
	p.MetricsManager, err = p.metricsProvider(metricCfg)
	if err != nil {
		return err
	}
	// Start the HTTP server for Prometheus scraping
	go func() {
		<-ctx.Done()
		if err := p.MetricsManager.HttpServer.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down metrics HTTP server: %v\n", err)
		}
	}()
	p.MetricsManager.SignalStart()
	return nil
}

func (p *Prometheus) RecordMetrics(metricName string, value float64) error {

	// 根据 metricName 决定要更新哪个指标
	switch metricName {
	case "execute_duration":
		p.MetricsManager.ExecuteDuration.Observe(value)
	case "parser_duration":
		p.MetricsManager.ParserDuration.Observe(value)
	case "optimize_duration":
		p.MetricsManager.OptimizeDuration.Observe(value)
	case "request_count_inc":
		// 如果是计数器类型的指标，假设每次调用增加 1
		p.MetricsManager.CounterMetric.Inc()
	case "request_count_add":
		// 如果是计数器类型的指标，并且需要增加特定的值
		p.MetricsManager.CounterMetric.Add(value)
	case "some_other_metric":
	default:
		return errors.Errorf("metrcis not found")
		// 更新其他指标
	}
	return nil
}

func (p *Prometheus) metricsProvider(metricCfg *config.Metric) (*MetricsManager, error) {
	// Create a new Prometheus registry
	registry := prometheus.NewRegistry()

	// Set up the MetricsManager with the registry
	metricsManager := NewMetricsManager(registry)

	// Optionally, use cfg to customize the MetricsManager or registry
	if len(metricCfg.Address) > 0 {
		metricsManager.HttpServer.Addr = metricCfg.Address
	}

	return metricsManager, nil
}

type Registry interface {
	prometheus.Registerer
	prometheus.Gatherer
}

type Counter interface {
	Inc()
	Add(float64)
}

type Gauge interface {
	Set(float64)
}

type Histogram interface {
	Observe(float64)
}

type Summary interface {
	Observe(float64)
}

type MetricsManager struct {
	registry            Registry
	startHttpServerChan chan bool
	stopHttpServerChan  chan bool
	HttpServer          *http.Server

	CounterMetric    Counter
	GaugeMetric      Gauge
	SummaryMetric    Summary
	HistogramMetric  Histogram
	ParserDuration   Histogram
	ExecuteDuration  Histogram
	OptimizeDuration Histogram
}

func (m *MetricsManager) UpdateCounter(value float64) {
	m.CounterMetric.Add(value)
}

func (m *MetricsManager) UpdateGauge(value float64) {
	m.GaugeMetric.Set(value)
}

func (m *MetricsManager) RecordHistogramObservation(value float64) {
	m.HistogramMetric.Observe(value)
}

func (m *MetricsManager) RecordSummaryObservation(value float64) {
	m.SummaryMetric.Observe(value)
}

// Additional methods to record metrics
func (m *MetricsManager) RecordParserDuration(duration float64) {
	m.ParserDuration.Observe(duration)
}

func (m *MetricsManager) RecordOptimizeDuration(duration float64) {
	m.OptimizeDuration.Observe(duration)
}

func (m *MetricsManager) RecordExecuteDuration(duration float64) {
	m.ExecuteDuration.Observe(duration)
}

func (m *MetricsManager) StartHttpServer() {
	go func() {
		<-m.startHttpServerChan
		m.HttpServer.Handler = promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
		if err := m.HttpServer.ListenAndServe(); err != http.ErrServerClosed {
			// Handle error - log or panic as appropriate
			fmt.Println(err)
		}
	}()
}

func (m *MetricsManager) SignalStart() {
	m.startHttpServerChan <- true
}

func (m *MetricsManager) SignalStop() {
	if err := m.HttpServer.Close(); err != nil {
		// Handle error - log or panic as appropriate
		fmt.Println(err)
	}
}

func NewMetricsManager(r Registry) *MetricsManager {
	// Ensure the passed registry also implements prometheus.Gatherer
	_, ok := r.(prometheus.Gatherer)
	if !ok {
		panic("provided registry does not implement prometheus.Gatherer")
	}
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "example_counter",
		Help: "An example counter metric",
	})
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "example_gauge",
		Help: "An example gauge metric",
	})
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "example_histogram",
		Help:    "An example histogram metric",
		Buckets: prometheus.LinearBuckets(1, 1, 5), // Example buckets
	})
	summary := prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "example_summary",
		Help:       "An example summary metric",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}, // Example objectives
	})

	parserDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "parser",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in parse SQL.",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 25), // 40us ~ 11min
	})

	optimizeDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "optimizer",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in optimizer.",
		Buckets:   prometheus.ExponentialBuckets(0.00004, 2, 25), // 40us ~ 11min
	})

	executeDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arana",
		Subsystem: "executor",
		Name:      "duration_seconds",
		Help:      "histogram of processing time (s) in execute.",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 30), // 100us ~ 15h,
	})

	// Registering the metrics
	r.MustRegister(counter)
	r.MustRegister(gauge)
	r.MustRegister(histogram)
	r.MustRegister(summary)
	r.MustRegister(parserDuration)
	r.MustRegister(optimizeDuration)
	r.MustRegister(executeDuration)
	return &MetricsManager{
		registry:            r,
		startHttpServerChan: make(chan bool, 1),
		stopHttpServerChan:  make(chan bool, 1),
		HttpServer: &http.Server{
			Addr:    ":9090",
			Handler: nil, // Set up during start
		},
		CounterMetric:    counter,
		GaugeMetric:      gauge,
		HistogramMetric:  histogram,
		SummaryMetric:    summary,
		ParserDuration:   parserDuration,
		ExecuteDuration:  executeDuration,
		OptimizeDuration: optimizeDuration,
	}
}
