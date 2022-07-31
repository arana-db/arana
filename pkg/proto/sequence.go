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

//go:generate mockgen -destination=../../testdata/mock_sequence.go -package=testdata . Sequence,SequenceManager
package proto

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrorNotSequenceType  = errors.New("sequence type not found")
	ErrorNotFoundSequence = errors.New("sequence instance not found")

	_defaultSequenceManager SequenceManager
)

func RegisterSequenceManager(l SequenceManager) {
	_defaultSequenceManager = l
}

func LoadSequenceManager() SequenceManager {
	cur := _defaultSequenceManager
	if cur == nil {
		return noopSequenceManager{}
	}
	return cur
}

func BuildAutoIncrementName(table string) string {
	return fmt.Sprintf("__arana_incr_%s", table)
}

type (
	RuntimeCtxKey = struct{}

	// SequenceSupplier Create the creator of Sequence
	SequenceSupplier func() EnhancedSequence

	SequenceConfig struct {
		Name   string
		Type   string
		Option map[string]string
	}

	// Sequence represents a global unique id generator.
	Sequence interface {
		// Acquire generates a next value in int64.
		Acquire(ctx context.Context) (int64, error)
		Reset() error
		Update() error
	}

	// EnhancedSequence represents a global unique id generator.
	EnhancedSequence interface {
		Sequence
		// Start start sequence instance.
		Start(ctx context.Context, option SequenceConfig) error
		// CurrentVal get sequence current id.
		CurrentVal() int64
		// Stop stops sequence.
		Stop() error
	}

	// SequenceManager represents the factory to create a Sequence by table name.
	SequenceManager interface {
		// CreateSequence creates one sequence instance
		CreateSequence(ctx context.Context, tenant, schema string, opt SequenceConfig) (Sequence, error)
		// GetSequence gets sequence instance by name
		GetSequence(ctx context.Context, tenant, schema, name string) (Sequence, error)
	}
)

// Record the list of Sequence plug -in through the registered self -registered
var suppliersRegistry = make(map[string]SequenceSupplier)

// RegisterSequence Register a Sequence plugin
func RegisterSequence(name string, supplier SequenceSupplier) {
	suppliersRegistry[name] = supplier
}

// GetSequenceSupplier returns SequenceSupplier.
func GetSequenceSupplier(name string) (SequenceSupplier, bool) {
	val, ok := suppliersRegistry[name]
	return val, ok
}

type noopSequenceManager struct{}

// CreateSequence creates one sequence instance
func (n noopSequenceManager) CreateSequence(ctx context.Context, tenant, schema string, opt SequenceConfig) (Sequence, error) {
	return nil, nil
}

// GetSequence gets sequence instance by name
func (n noopSequenceManager) GetSequence(ctx context.Context, tenant, schema, name string) (Sequence, error) {
	return nil, nil
}
