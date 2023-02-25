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
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
)

const (
	_flagDirect cFlag = 1 << iota
	_flagRead
	_flagWrite
)

type (
	keyFlag           struct{}
	keyNodeLabel      struct{}
	keyDefaultDBGroup struct{}
	keyHints          struct{}
	keyTransactionID  struct{}
)

type cFlag uint8

// WithTransactionID sets transaction id
func WithTransactionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyTransactionID{}, id)
}

// WithNodeLabel sets the database node label, it will be used for node select.
// For Example, use `WithNodeLabel(ctx, "zone:shanghai")` if you want to choose nodes in Shanghai DC.
func WithNodeLabel(ctx context.Context, label string) context.Context {
	return context.WithValue(ctx, keyNodeLabel{}, label)
}

// WithDirect execute sql directly.
func WithDirect(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagDirect|getFlag(ctx))
}

// WithWrite marked as write operation
func WithWrite(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagWrite|getFlag(ctx))
}

// WithRead marked as read operation
func WithRead(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagRead|getFlag(ctx))
}

// WithHints binds the hints.
func WithHints(ctx context.Context, hints []*hint.Hint) context.Context {
	return context.WithValue(ctx, keyHints{}, hints)
}

// Tenant extracts the tenant.
func Tenant(ctx context.Context) string {
	tenant, ok := ctx.Value(proto.ContextKeyTenant{}).(string)
	if !ok {
		return ""
	}
	return tenant
}

// IsRead returns true if this is a read operation
func IsRead(ctx context.Context) bool {
	return hasFlag(ctx, _flagRead)
}

// IsWrite returns true if this is a write operation
func IsWrite(ctx context.Context) bool {
	return hasFlag(ctx, _flagWrite)
}

// IsDirect returns true if execute directly.
func IsDirect(ctx context.Context) bool {
	return hasFlag(ctx, _flagDirect)
}

// SQL returns the original sql string.
func SQL(ctx context.Context) string {
	if sql, ok := ctx.Value(proto.ContextKeySQL{}).(string); ok {
		return sql
	}
	return ""
}

func Schema(ctx context.Context) string {
	if schema, ok := ctx.Value(proto.ContextKeySchema{}).(string); ok {
		return schema
	}
	return ""
}

func Version(ctx context.Context) string {
	if schema, ok := ctx.Value(proto.ContextKeyServerVersion{}).(string); ok {
		return schema
	}
	return ""
}

// NodeLabel returns the label of node.
func NodeLabel(ctx context.Context) string {
	if label, ok := ctx.Value(keyNodeLabel{}).(string); ok {
		return label
	}
	return ""
}

// TransactionID returns the transactions id
func TransactionID(ctx context.Context) string {
	if label, ok := ctx.Value(keyTransactionID{}).(string); ok {
		return label
	}
	return ""
}

// Hints extracts the hints.
func Hints(ctx context.Context) []*hint.Hint {
	hints, ok := ctx.Value(keyHints{}).([]*hint.Hint)
	if !ok {
		return nil
	}
	return hints
}

func TransientVariables(ctx context.Context) map[string]proto.Value {
	if val, ok := ctx.Value(proto.ContextKeyTransientVariables{}).(map[string]proto.Value); ok {
		return val
	}
	return nil
}

func hasFlag(ctx context.Context, flag cFlag) bool {
	return getFlag(ctx)&flag != 0
}

func getFlag(ctx context.Context) cFlag {
	f, ok := ctx.Value(keyFlag{}).(cFlag)
	if !ok {
		return 0
	}
	return f
}
