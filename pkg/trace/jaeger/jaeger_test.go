package jaeger

import (
	"context"
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"testing"
)

func TestJaegerProvider(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tCtx := context.Background()
	tTraceCfg := &config.Trace{
		Type:    "jaeger",
		Address: "http://localhost:14268/api/traces",
	}

	tPCtx := &proto.Context{
		Context: tCtx,
	}
	tHint := []*hint.Hint{
		{
			Type: hint.TypeTrace,
			Inputs: []hint.KeyValue{{
				K: "test_hint_key",
				V: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
			}},
		},
	}

	j := &Jaeger{}

	// test for initialize and set provider to jaeger
	err := j.Initialize(tCtx, tTraceCfg)
	assert.NoError(t, err)

	// test get provider from jaeger
	tTp := otel.GetTracerProvider()
	assert.NotNil(t, tTp)

	// test extract hint by jaeger
	result := j.Extract(tPCtx, tHint)
	assert.Equal(t, true, result)

	// test extracted content to ctx
	assert.NotEqual(t, tCtx, tPCtx.Context)

	// test extract hint by jaeger
	failResult := j.Extract(tPCtx, []*hint.Hint{{
		Type:   hint.TypeDirect,
		Inputs: nil,
	}})
	assert.Equal(t, false, failResult)
}
