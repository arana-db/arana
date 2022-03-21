// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package context

import (
	"context"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
)

const (
	_flagMaster cFlag = 1 << iota
	_flagSlave
	_flagDirect
)

type (
	keyFlag      struct{}
	keyRule      struct{}
	keySequence  struct{}
	keySql       struct{}
	keyNodeLabel struct{}
)

type cFlag uint8

// WithNodeLabel sets the database node label, it will be used for node select.
// For Example, use `WithNodeLabel(ctx, "zone:shanghai")` if you want to choose nodes in Shanghai DC.
func WithNodeLabel(ctx context.Context, label string) context.Context {
	return context.WithValue(ctx, keyNodeLabel{}, label)
}

// WithDirect execute sql directly.
func WithDirect(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagDirect|getFlag(ctx))
}

// WithSQL binds the original sql.
func WithSQL(ctx context.Context, sql string) context.Context {
	return context.WithValue(ctx, keySql{}, sql)
}

// WithMaster uses master datasource.
func WithMaster(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagMaster|getFlag(ctx))
}

// WithSlave uses slave datasource.
func WithSlave(ctx context.Context) context.Context {
	return context.WithValue(ctx, keyFlag{}, _flagSlave|getFlag(ctx))
}

// WithRule binds a rule.
func WithRule(ctx context.Context, ru *rule.Rule) context.Context {
	return context.WithValue(ctx, keyRule{}, ru)
}

// WithSequencer binds a sequencer.
func WithSequencer(ctx context.Context, sequencer proto.Sequencer) context.Context {
	return context.WithValue(ctx, keySequence{}, sequencer)
}

// Sequencer extracts the sequencer.
func Sequencer(ctx context.Context) proto.Sequencer {
	s, ok := ctx.Value(keySequence{}).(proto.Sequencer)
	if !ok {
		return nil
	}
	return s
}

// Rule extracts the rule.
func Rule(ctx context.Context) *rule.Rule {
	ru, ok := ctx.Value(keyRule{}).(*rule.Rule)
	if !ok {
		return nil
	}
	return ru
}

// IsMaster returns true if force using master.
func IsMaster(ctx context.Context) bool {
	return hasFlag(ctx, _flagMaster)
}

// IsSlave returns true if force using master.
func IsSlave(ctx context.Context) bool {
	return hasFlag(ctx, _flagSlave)
}

// IsDirect returns true if execute directly.
func IsDirect(ctx context.Context) bool {
	return hasFlag(ctx, _flagDirect)
}

// SQL returns the original sql string.
func SQL(ctx context.Context) string {
	if sql, ok := ctx.Value(keySql{}).(string); ok {
		return sql
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
