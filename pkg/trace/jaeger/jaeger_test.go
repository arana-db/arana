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
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"

	"go.opentelemetry.io/otel"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
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
