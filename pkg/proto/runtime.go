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

//go:generate mockgen -destination=../../testdata/mock_runtime.go -package=testdata . VConn,Plan,Optimizer,DB
package proto

import (
	"context"
	"io"
	"time"
)

import (
	"github.com/arana-db/parser/ast"
)

const (
	PlanTypeQuery PlanType = iota // QUERY
	PlanTypeExec                  // EXEC
)

type (
	// VConn represents a virtual connection which can be used to query/exec from a db.
	VConn interface {
		// Query requests a query command.
		Query(ctx context.Context, db string, query string, args ...interface{}) (Result, error)
		// Exec requests a exec command
		Exec(ctx context.Context, db string, query string, args ...interface{}) (Result, error)
	}

	// PlanType represents the type of Plan.
	PlanType uint8

	// Plan represents a plan for query/execute command.
	Plan interface {
		// Type returns the type of Plan.
		Type() PlanType
		// ExecIn executes the current Plan.
		ExecIn(ctx context.Context, conn VConn) (Result, error)
	}

	// Optimizer represents a sql statement optimizer which can be used to create QueryPlan or ExecPlan.
	Optimizer interface {
		// Optimize optimizes the sql with arguments then returns a Plan.
		Optimize(ctx context.Context, stmt ast.StmtNode, args ...interface{}) (Plan, error)
	}

	// Weight represents the read/write weight info.
	Weight struct {
		R int32 // read weight
		W int32 // write weight
	}

	// Callable represents sql caller.
	Callable interface {
		// Call executes a sql.
		Call(ctx context.Context, sql string, args ...interface{}) (res Result, warn uint16, err error)
		// CallFieldList lists fields.
		CallFieldList(ctx context.Context, table, wildcard string) ([]Field, error)
	}

	// DB represents an accessor to physical mysql, just like sql.DB.
	DB interface {
		io.Closer
		Callable
		// ID returns the unique id.
		ID() string
		// IdleTimeout returns the idle timeout.
		IdleTimeout() time.Duration
		// MaxCapacity returns the max capacity.
		MaxCapacity() int
		// Capacity returns the capacity.
		Capacity() int
		// Weight returns the weight.
		Weight() Weight
		// SetCapacity sets the capacity.
		SetCapacity(capacity int) error
		// SetMaxCapacity sets the max capacity.
		SetMaxCapacity(maxCapacity int) error
		// SetIdleTimeout sets the idle timeout.
		SetIdleTimeout(idleTimeout time.Duration) error
		// SetWeight sets the weight.
		SetWeight(weight Weight) error
	}

	// Executable represents an executor which can send sql request.
	Executable interface {
		// Execute executes the sql context.
		Execute(ctx *Context) (result Result, warn uint16, err error)
	}

	// Tx represents transaction.
	Tx interface {
		Executable
		// ID returns the unique transaction id.
		ID() int64
		// Commit commits current transaction.
		Commit(ctx context.Context) (Result, uint16, error)
		// Rollback rollbacks current transaction.
		Rollback(ctx context.Context) (Result, uint16, error)
	}
)
