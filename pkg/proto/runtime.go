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

//go:generate mockgen -destination=../../testdata/mock_runtime.go -package=testdata . Rows,VConn,MixinResult,Plan,Optimizer
package proto

import (
	"context"
)

const (
	PlanTypeQuery PlanType = iota // QUERY
	PlanTypeExec                  // EXEC
)

type (
	// Rows represents a flow of Row.
	Rows interface {
		// Next returns the next Row, nil if EOF.
		Next() Row
	}

	// MixinResult mixin the Rows and Result.
	MixinResult interface {
		Rows
		Result
	}

	// VConn represents a virtual connection which can be used to query/exec from a db.
	VConn interface {
		// Query requests a query command.
		Query(ctx context.Context, db string, query string, args ...interface{}) (Rows, error)
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
		ExecIn(ctx context.Context, conn VConn) (MixinResult, error)
	}

	// Optimizer represents a sql statement optimizer which can be used to create QueryPlan or ExecPlan.
	Optimizer interface {
		// Optimize optimizes the sql with arguments then returns a Plan.
		Optimize(ctx context.Context, sql string, args ...interface{}) (Plan, error)
	}
)
