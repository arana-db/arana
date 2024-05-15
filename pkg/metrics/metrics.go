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

	"sync"

	"github.com/arana-db/arana/pkg/config"
	"github.com/pkg/errors"
	// Other necessary imports...
)

const (
	Prometheus ProviderType = "prometheus"
)

// ProviderType is a string alias representing the type of Prometheus provider.
type ProviderType string

// A map to store registered providers
var (
	providers = make(map[ProviderType]Provider)
	// currentProvider holds the currently active Prometheus provider.
	currentProvider Provider
	once            sync.Once
)

// RegisterProviders registers a new Prometheus provider.
func RegisterProviders(pType ProviderType, p Provider) {
	providers[pType] = p
}

// Initialize sets up the Prometheus provider based on the provided configuration.
func Initialize(ctx context.Context, cfg *config.Metric) error {
	var err error
	once.Do(func() {
		provider, ok := providers[ProviderType(cfg.Type)]
		if !ok {
			err = errors.Errorf("not supported %s  metrics provider ", cfg.Type)
			return
		}
		currentProvider = provider
		err = currentProvider.Initialize(ctx, cfg)
	})
	return err
}

func RecordMetrics(metricName string, value float64) error {
	return currentProvider.RecordMetrics(metricName, value)
}

// Provider interface defines the methods that a Prometheus provider must implement.
type Provider interface {
	Initialize(ctx context.Context, cfg *config.Metric) error
	RecordMetrics(metricName string, value float64) error
}
