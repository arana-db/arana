package trace

import (
	"context"
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

type TestProvider struct{}

func (t *TestProvider) Initialize(_ context.Context, traceCfg *config.Trace) error {
	return nil
}

func (t *TestProvider) Extract(ctx *proto.Context, hints []*hint.Hint) bool {
	return false
}

func TestUseTraceProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tCtx := context.Background()
	tPCtx := &proto.Context{}
	tProviderType := "test_provider"
	tTraceCfg := &config.Trace{
		Type:    tProviderType,
		Address: "test_address",
	}

	tTraceCfgF := &config.Trace{
		Type:    "test_provider_2",
		Address: "test_address",
	}

	// test for Initialize fail without Register
	err := Initialize(tCtx, tTraceCfgF)
	assert.Error(t, err)
	once = sync.Once{}

	// test for Register ability
	tProvider := &TestProvider{}
	RegisterProviders(ProviderType(tProviderType), tProvider)
	assert.Equal(t, tProvider, providers[ProviderType(tProviderType)])

	// test for Initialize ability
	err = Initialize(tCtx, tTraceCfg)
	assert.NoError(t, err)

	// test for Extract ability
	hasExt := Extract(tPCtx, nil)
	assert.Equal(t, false, hasExt)
}
