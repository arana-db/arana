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

package optimize

import (
	"context"
	stdErrors "errors"
	"strings"
)

import (
	"github.com/arana-db/parser/ast"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	rast "github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Optimizer = (*optimizer)(nil)

// errors group
var (
	errNoRuleFound  = stdErrors.New("no rule found")
	errDenyFullScan = stdErrors.New("the full-scan query is not allowed")
)

// IsNoRuleFoundErr returns true if target error is caused by NO-RULE-FOUND.
func IsNoRuleFoundErr(err error) bool {
	return errors.Is(err, errNoRuleFound)
}

// IsDenyFullScanErr returns true if target error is caused by DENY-FULL-SCAN.
func IsDenyFullScanErr(err error) bool {
	return errors.Is(err, errDenyFullScan)
}

func GetOptimizer() proto.Optimizer {
	return optimizer{}
}

type optimizer struct {
}

func (o optimizer) Optimize(ctx context.Context, stmt ast.StmtNode, args ...interface{}) (plan proto.Plan, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.Errorf("cannot analyze sql %s", rcontext.SQL(ctx))
			log.Errorf("optimize panic: sql=%s, rec=%v", rcontext.SQL(ctx), rec)
		}
	}()

	var xxstmt rast.Statement
	if xxstmt, err = rast.FromStmtNode(stmt); err != nil {
		return nil, errors.Wrap(err, "optimize failed")
	}
	return o.doOptimize(ctx, xxstmt, args...)
}

func (o optimizer) doOptimize(ctx context.Context, stmt rast.Statement, args ...interface{}) (proto.Plan, error) {
	switch t := stmt.(type) {
	case *rast.SelectStatement:
		return o.optimizeSelect(ctx, t, args)
	case *rast.InsertStatement:
	case *rast.DeleteStatement:
	case *rast.UpdateStatement:
	}

	//TODO implement all statements
	panic("implement me")
}

const (
	_bypass uint32 = 1 << iota
	_supported
)

func (o optimizer) getSelectFlag(ctx context.Context, stmt *rast.SelectStatement) (flag uint32) {
	switch len(stmt.From) {
	case 1:
		from := stmt.From[0]
		tn := from.TableName()

		if tn == nil { // only FROM table supported now
			return
		}

		flag |= _supported

		if len(tn) > 1 {
			switch strings.ToLower(tn.Prefix()) {
			case "mysql", "information_schema":
				flag |= _bypass
				return
			}
		}
		if !rcontext.Rule(ctx).Has(tn.Suffix()) {
			flag |= _bypass
		}
	case 0:
		flag |= _bypass
		flag |= _supported
	}
	return
}

func (o optimizer) optimizeSelect(ctx context.Context, stmt *rast.SelectStatement, args []interface{}) (proto.Plan, error) {
	var ru *rule.Rule
	if ru = rcontext.Rule(ctx); ru == nil {
		return nil, errors.WithStack(errNoRuleFound)
	}

	flag := o.getSelectFlag(ctx, stmt)
	if flag&_supported == 0 {
		return nil, errors.Errorf("unsupported sql: %s", rcontext.SQL(ctx))
	}

	if flag&_bypass != 0 {
		return &plan.SimpleQueryPlan{Stmt: stmt, Args: args}, nil
	}

	shards, fullScan, err := (*Sharder)(ru).Shard(stmt.From[0].TableName(), stmt.Where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "calculate shards failed")
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	if fullScan {
		return nil, errors.WithStack(err)
	}

	if len(shards) == 1 {
		for k, v := range shards {
			return &plan.SimpleQueryPlan{
				Database: k,
				Tables:   v,
				Stmt:     stmt,
				Args:     args,
			}, nil
		}
	}

	plans := make([]proto.Plan, 0, len(shards))
	for k, v := range shards {
		plans = append(plans, &plan.SimpleQueryPlan{
			Database: k,
			Tables:   v,
			Stmt:     stmt,
			Args:     args,
		})
	}

	unionPlan := &plan.UnionPlan{
		Plans: plans,
	}
	// TODO: order/groupBy/aggregate

	return unionPlan, nil
}
