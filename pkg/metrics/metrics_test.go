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
	"testing"

	"github.com/arana-db/arana/pkg/config"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type TestProvider struct{}

func (t *TestProvider) Initialize(_ context.Context, traceCfg *config.Metric) error {
	return nil
}

func (t *TestProvider) RecordMetrics(metricName string, value float64) error {
	return nil
}

func TestUsePrometheusProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tCtx := context.Background()
	tProviderType := "test_type1"
	tmetricCfg := &config.Metric{
		Type:           tProviderType,
		Address:        "http://localhost:9090",
		ScrapeInterval: "15s",
		ScrapeTimeout:  "10s",
	}
	// test for Register ability
	tProvider := &TestProvider{}
	RegisterProviders(ProviderType(tProviderType), tProvider)
	assert.Equal(t, tProvider, providers[ProviderType(tProviderType)])

	// test for Initialize ability
	err := Initialize(tCtx, tmetricCfg)
	assert.NoError(t, err)

	// test for Extract ability
	hasExt := RecordMetrics("", 1)
	assert.Equal(t, nil, hasExt)
}
