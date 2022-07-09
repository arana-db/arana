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

package optimize

import (
	"context"
	"errors"
)

import (
	perrors "github.com/pkg/errors"

	"go.opentelemetry.io/otel"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/proto/rule"
	rast "github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/parser/ast"
)

var _ proto.Optimizer = (*optimizer)(nil)

var Tracer = otel.Tracer("optimize")

// errors group
var (
	errNoRuleFound     = errors.New("optimize: no rule found")
	errDenyFullScan    = errors.New("optimize: the full-scan query is not allowed")
	errNoShardKeyFound = errors.New("optimize: no shard key found")
)

// IsNoShardKeyFoundErr returns true if target error is caused by NO-SHARD-KEY-FOUND
func IsNoShardKeyFoundErr(err error) bool {
	return perrors.Is(err, errNoShardKeyFound)
}

// IsNoRuleFoundErr returns true if target error is caused by NO-RULE-FOUND.
func IsNoRuleFoundErr(err error) bool {
	return perrors.Is(err, errNoRuleFound)
}

// IsDenyFullScanErr returns true if target error is caused by DENY-FULL-SCAN.
func IsDenyFullScanErr(err error) bool {
	return perrors.Is(err, errDenyFullScan)
}

type optimizer struct {
	rule  *rule.Rule
	hints []*hint.Hint
	stmt  rast.Statement
	args  []interface{}
}

func NewOptimizer(rule *rule.Rule, hints []*hint.Hint, stmt ast.StmtNode, args []interface{}) (proto.Optimizer, error) {
	var (
		rstmt rast.Statement
		err   error
	)
	if rstmt, err = rast.FromStmtNode(stmt); err != nil {
		return nil, perrors.Wrap(err, "optimize failed")
	}

	return &optimizer{
		rule:  rule,
		hints: hints,
		stmt:  rstmt,
		args:  args,
	}, nil
}

type optimizeHandler func(ctx context.Context, o *optimizer) (proto.Plan, error)

var (
	_handlers = make(map[rast.SQLType]optimizeHandler)
)

func registerOptimizeHandler(t rast.SQLType, h optimizeHandler) {
	_handlers[t] = h
}

func init() {
	registerOptimizeHandler(rast.SQLTypeAlterTable, optimizeAlterTable)
}

func (o *optimizer) Optimize(ctx context.Context) (plan proto.Plan, err error) {
	ctx, span := Tracer.Start(ctx, "Optimize")
	defer func() {
		span.End()
		if rec := recover(); rec != nil {
			err = perrors.Errorf("cannot analyze sql %s", rcontext.SQL(ctx))
			log.Errorf("optimize panic: sql=%s, rec=%v", rcontext.SQL(ctx), rec)
		}
	}()

	h, ok := _handlers[o.stmt.Mode()]
	if !ok {
		return nil, perrors.Errorf("optimize: no handler found for '%s'", o.stmt.Mode())
	}

	return h(ctx, o)
}

func (o optimizer) computeShards(table rast.TableName, where rast.ExpressionNode, args []interface{}) (rule.DatabaseTables, error) {
	ru := o.rule
	vt, ok := ru.VTable(table.Suffix())
	if !ok {
		return nil, nil
	}

	shards, fullScan, err := (*Sharder)(ru).Shard(table, where, args...)
	if err != nil {
		return nil, perrors.Wrap(err, "calculate shards failed")
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	// return error if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, perrors.WithStack(errDenyFullScan)
	}

	if shards.IsEmpty() {
		return shards, nil
	}

	if len(shards) == 0 {
		// compute all tables
		shards = vt.Topology().Enumerate()
	}

	return shards, nil
}
