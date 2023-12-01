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
