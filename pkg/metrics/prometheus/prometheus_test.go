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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestMetricsManagerStart(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	metricsManager := NewMetricsManager(registry)

	metricsManager.StartHttpServer()
	metricsManager.SignalStart()
	// Add a short delay to ensure the server starts up
	time.Sleep(100 * time.Millisecond)

	// Use httptest to create a test server with the same handler
	testServer := httptest.NewServer(metricsManager.HttpServer.Handler)
	defer testServer.Close()

	// Perform a test HTTP request to the test server
	resp, err := http.Get(testServer.URL + "/metrics")
	if err != nil {
		t.Fatalf("Failed to make request to the test server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected OK status code, got %v", resp.StatusCode)
	}
}

func TestMetricsManagerUpdate(t *testing.T) {
	// Create an isolated Prometheus registry for testing
	testRegistry := prometheus.NewPedanticRegistry()

	// Create the MetricsManager with the test registry
	manager := NewMetricsManager(testRegistry)

	// Test if metrics are registered correctly
	mfs, err := testRegistry.Gather()
	assert.NoError(t, err)
	assert.Len(t, mfs, 7)

	// Test updating Counter
	manager.UpdateCounter(1)
	assertCounterMetric(t, testRegistry, "example_counter", 1)

	// Test updating Gauge
	manager.UpdateGauge(5)
	assertGaugeMetric(t, testRegistry, "example_gauge", 5)

	// Test recording histogram observation
	histogramTestValue := 2.5
	manager.RecordHistogramObservation(histogramTestValue)
	assertHistogramMetric(t, testRegistry, "example_histogram", 1, histogramTestValue)

	// Test recording summary observation
	summaryTestValue := 3.5
	manager.RecordSummaryObservation(summaryTestValue)
	assertSummaryMetric(t, testRegistry, "example_summary", 1, summaryTestValue)

	parserDurationTestValue := 2.5
	manager.RecordParserDuration(parserDurationTestValue)
	assertHistogramMetric(t, testRegistry, "arana_parser_duration_seconds", 1, parserDurationTestValue)

	optimizeDurationTestValue := 2.5
	manager.RecordOptimizeDuration(optimizeDurationTestValue)
	assertHistogramMetric(t, testRegistry, "arana_optimizer_duration_seconds", 1, optimizeDurationTestValue)

	executeDurationTestValue := 2.5
	manager.RecordExecuteDuration(executeDurationTestValue)
	assertHistogramMetric(t, testRegistry, "arana_executor_duration_seconds", 1, executeDurationTestValue)
}

func assertCounterMetric(t *testing.T, r prometheus.Gatherer, metricName string, expectedValue float64) {
	mfs, err := r.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			metric := mf.GetMetric()[0]
			assert.Equal(t, expectedValue, metric.GetCounter().GetValue())
			return
		}
	}
	t.Errorf("Metric %s not found", metricName)
}

func assertGaugeMetric(t *testing.T, r prometheus.Gatherer, metricName string, expectedValue float64) {
	mfs, err := r.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			metric := mf.GetMetric()[0]
			assert.Equal(t, expectedValue, metric.GetGauge().GetValue())
			return
		}
	}
	t.Errorf("Metric %s not found", metricName)
}

func assertHistogramMetric(t *testing.T, r prometheus.Gatherer, metricName string, expectedCount int, expectedSum float64) {
	mfs, err := r.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			metric := mf.GetMetric()[0]
			histogram := metric.GetHistogram()
			assert.Equal(t, float64(expectedCount), float64(histogram.GetSampleCount()))
			assert.Equal(t, expectedSum, histogram.GetSampleSum())
			return
		}
	}
	t.Errorf("Metric %s not found", metricName)
}

func assertSummaryMetric(t *testing.T, r prometheus.Gatherer, metricName string, expectedCount int, expectedSum float64) {
	mfs, err := r.Gather()
	assert.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			metric := mf.GetMetric()[0]
			summary := metric.GetSummary()
			assert.Equal(t, uint64(expectedCount), summary.GetSampleCount())
			assert.Equal(t, expectedSum, summary.GetSampleSum())
			return
		}
	}
	t.Errorf("Metric %s not found", metricName)
}
