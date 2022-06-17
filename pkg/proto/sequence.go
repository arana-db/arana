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

type (
	VConnCtxKey = struct{}
)

// SequenceSupplier Create the creator of Sequence
type SequenceSupplier func() EnchanceSequence

var (
	// Record the list of Sequence plug -in through the registered self -registered
	suppliersRegistry map[string]SequenceSupplier = map[string]SequenceSupplier{}
)

// RegisterSequence Register a Sequence plugin
func RegisterSequence(name string, supplier SequenceSupplier) {
	suppliersRegistry[name] = supplier
}

type SequenceConfig struct {
	Name   string
	Type   string
	Option map[string]string
}

// Sequence represents a global unique id generator.
type EnchanceSequence interface {
	Sequence
	// Start start sequence instance
	Start(ctx context.Context, option SequenceConfig) error
	// CurrentVal get sequence current id
	CurrentVal() int64
	// Stop stop sequence
	Stop() error
}

// Sequencer represents the factory to create a Sequence by table name.
type SequenceManager interface {
	// CreateSequence creates one sequence instance
	CreateSequence(ctx context.Context, conn VConn, opt SequenceConfig) (Sequence, error)
	// GetSequence gets sequence instance by name
	GetSequence(ctx context.Context, name string) (Sequence, error)
}

// GetSequenceSupplier
func GetSequenceSupplier(name string) (SequenceSupplier, bool) {
	val, ok := suppliersRegistry[name]
	return val, ok
}
