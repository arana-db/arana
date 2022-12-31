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
	"github.com/arana-db/parser/ast"

	perrors "github.com/pkg/errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/proto/rule"
	rast "github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Optimizer = (*Optimizer)(nil)

var Tracer = otel.Tracer("optimize")

// errors group
var (
	ErrNoRuleFound     = errors.New("optimize: no rule found")
	ErrDenyFullScan    = errors.New("optimize: the full-scan query is not allowed")
	ErrNoShardKeyFound = errors.New("optimize: no shard key found")
)

// IsNoShardKeyFoundErr returns true if target error is caused by NO-SHARD-KEY-FOUND
func IsNoShardKeyFoundErr(err error) bool {
	return perrors.Is(err, ErrNoShardKeyFound)
}

// IsNoRuleFoundErr returns true if target error is caused by NO-RULE-FOUND.
func IsNoRuleFoundErr(err error) bool {
	return perrors.Is(err, ErrNoRuleFound)
}

// IsDenyFullScanErr returns true if target error is caused by DENY-FULL-SCAN.
func IsDenyFullScanErr(err error) bool {
	return perrors.Is(err, ErrDenyFullScan)
}

var _handlers = make(map[rast.SQLType]Processor)

func Register(t rast.SQLType, h Processor) {
	_handlers[t] = h
}

type Processor = func(ctx context.Context, o *Optimizer) (proto.Plan, error)

type Optimizer struct {
	Rule  *rule.Rule
	Hints []*hint.Hint
	Stmt  rast.Statement
	Args  []proto.Value
}

func NewOptimizer(rule *rule.Rule, hints []*hint.Hint, stmt ast.StmtNode, args []proto.Value) (proto.Optimizer, error) {
	var (
		rstmt rast.Statement
		err   error
	)
	if rstmt, err = rast.FromStmtNode(stmt); err != nil {
		return nil, perrors.Wrap(err, "optimize failed")
	}

	return &Optimizer{
		Rule:  rule,
		Hints: hints,
		Stmt:  rstmt,
		Args:  args,
	}, nil
}

func (o *Optimizer) Optimize(ctx context.Context) (plan proto.Plan, err error) {
	ctx, span := Tracer.Start(ctx, "Optimize")
	span.SetAttributes(attribute.Key("sql.type").String(o.Stmt.Mode().String()))
	defer func() {
		span.End()
		if rec := recover(); rec != nil {
			err = perrors.Errorf("cannot analyze sql %s", rcontext.SQL(ctx))
			log.Errorf("optimize panic: sql=%s, rec=%v", rcontext.SQL(ctx), rec)
		}
	}()

	h, ok := _handlers[o.Stmt.Mode()]
	if !ok {
		return nil, perrors.Errorf("optimize: no handler found for '%s'", o.Stmt.Mode())
	}

	return h(ctx, o)
}

func (o *Optimizer) ComputeShards(table rast.TableName, where rast.ExpressionNode, args []proto.Value) (rule.DatabaseTables, error) {
	ru := o.Rule
	vt, ok := ru.VTable(table.Suffix())
	if !ok {
		return nil, nil
	}
	var (
		shards   rule.DatabaseTables
		err      error
		fullScan bool
	)

	if len(o.Hints) > 0 {
		if shards, err = Hints(table, o.Hints, o.Rule); err != nil {
			return nil, perrors.Wrap(err, "calculate hints failed")
		}
	}

	if shards == nil {
		xsd := NewXSharder(ru, args)
		if err := xsd.ForSingleSelect(table, "", where); err != nil {
			return nil, perrors.Wrapf(err, "optimize: cannot calculate shards of table '%s'", table.Suffix())
		}
		for i := range xsd.results {
			if xsd.results[i].L.Suffix() == table.Suffix() {
				if r := xsd.results[i].R; r != nil {
					shards = make(rule.DatabaseTables)
					r.Each(func(db, tb uint32) bool {
						dbs, tbs, ok := vt.Topology().Render(int(db), int(tb))
						if !ok {
							err = perrors.Errorf("cannot render table '%s'", vt.Name())
							return false
						}
						shards[dbs] = append(shards[dbs], tbs)
						return true
					})
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	fullScan = shards.IsFullScan()

	// return error if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, perrors.WithStack(ErrDenyFullScan)
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
