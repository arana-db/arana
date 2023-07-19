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

package jaeger

import (
	"context"
)

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/trace"
)

const (
	parentKey = "traceparent"
)

type Jaeger struct{}

func init() {
	trace.RegisterProviders(trace.Jaeger, &Jaeger{})
}

func (j *Jaeger) Initialize(_ context.Context, traceCfg *config.Trace) error {
	tp, err := j.tracerProvider(traceCfg)
	if err != nil {
		return err
	}
	// Register our TracerProvider as the global so any imported
	// instrumentation in the future will default to using it.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(&propagation.TraceContext{})
	return nil
}

func (j *Jaeger) tracerProvider(traceCfg *config.Trace) (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(traceCfg.Address)))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(trace.Service),
		)),
	)
	return tp, nil
}

func (j *Jaeger) Extract(ctx *proto.Context, hints []*hint.Hint) bool {
	var (
		traceContext string
	)
	for _, h := range hints {
		if h.Type != hint.TypeTrace {
			continue
		}
		traceContext = h.Inputs[0].V
		break
	}
	if len(traceContext) == 0 {
		return false
	}
	ctx.Context = otel.GetTextMapPropagator().Extract(ctx.Context, propagation.MapCarrier{parentKey: traceContext})
	return true
}
