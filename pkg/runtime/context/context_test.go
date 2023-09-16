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

package context

import (
	"context"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
)

func TestContext(t *testing.T) {
	ctx := context.Background()

	id := "1024"
	assert.Equal(t, TransactionID(WithTransactionID(ctx, id)), id)

	label := "arana"
	assert.Equal(t, NodeLabel(WithNodeLabel(ctx, label)), label)

	res, err := hint.Parse("master")
	assert.NoError(t, err)
	hints := []*hint.Hint{res}
	assert.Empty(t, Hints(ctx))
	assert.Equal(t, Hints(WithHints(ctx, hints)), hints)

	assert.Empty(t, TransientVariables(ctx))
	value, err := proto.NewValue("arana")
	assert.NoError(t, err)
	variables := map[string]proto.Value{"arana": value}
	variablesCtx := context.WithValue(ctx, proto.ContextKeyTransientVariables{}, variables)
	assert.Equal(t, TransientVariables(variablesCtx), variables)

	assert.True(t, IsDirect(WithDirect(ctx)))
	assert.True(t, IsWrite(WithWrite(ctx)))
	assert.True(t, IsRead(WithRead(ctx)))

	assert.Empty(t, Tenant(ctx))
	assert.Empty(t, SQL(ctx))
	assert.Empty(t, Schema(ctx))
	assert.Empty(t, Version(ctx))
}
