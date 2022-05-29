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

//go:generate mockgen -destination=../../testdata/mock_sequence.go -package=testdata . Sequence,Sequencer
package proto

import (
	"context"
)

var (
	ContextVconnKey = struct{}{}
)

var (
	createSequenceManager func() SequenceManager
)

func SetSequenceManagerCreator(creator func() SequenceManager) {
	createSequenceManager = creator
}

// SequenceSupplier 创建 Sequence 的创建器
type SequenceSupplier func() EnchanceSequence

var (
	// 记录通过 init 自注册的 sequence 插件列表
	suppliersRegistry map[string]SequenceSupplier = map[string]SequenceSupplier{}
)

// RegisterSequence 注册一个 sequence 插件
func RegisterSequence(name string, supplier SequenceSupplier) {
	suppliersRegistry[name] = supplier
}

func GetSequenceManager() SequenceManager {
	return createSequenceManager()
}

type SequenceConfig struct {
	Name   string
	Type   string
	Option map[string]string
}

// Sequence represents a global unique id generator.
type EnchanceSequence interface {
	Sequence
	// Start
	Start(ctx context.Context, option SequenceConfig) error
	// Stop
	Stop() error
}

// Sequencer represents the factory to create a Sequence by table name.
type SequenceManager interface {
	// CreateSequence
	CreateSequence(ctx context.Context, conn VConn, opt SequenceConfig) (Sequence, error)
	// GetSequence returns the Sequence of table.
	GetSequence(ctx context.Context, table string) (Sequence, error)
}

func GetSequenceSupplier(name string) (SequenceSupplier, bool) {
	val, ok := suppliersRegistry[name]
	return val, ok
}
