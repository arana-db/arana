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
	"github.com/arana-db/arana/pkg/runtime/cmp"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Optimizer = (*optimizer)(nil)

// errors group
var (
	errNoRuleFound     = stdErrors.New("no rule found")
	errDenyFullScan    = stdErrors.New("the full-scan query is not allowed")
	errNoShardKeyFound = stdErrors.New("no shard key found")
)

// IsNoShardKeyFoundErr returns true if target error is caused by NO-SHARD-KEY-FOUND
func IsNoShardKeyFoundErr(err error) bool {
	return errors.Is(err, errNoShardKeyFound)
}

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

	var rstmt rast.Statement
	if rstmt, err = rast.FromStmtNode(stmt); err != nil {
		return nil, errors.Wrap(err, "optimize failed")
	}
	return o.doOptimize(ctx, rstmt, args...)
}

func (o optimizer) doOptimize(ctx context.Context, stmt rast.Statement, args ...interface{}) (proto.Plan, error) {
	switch t := stmt.(type) {
	case *rast.SelectStatement:
		return o.optimizeSelect(ctx, t, args)
	case *rast.InsertStatement:
		return o.optimizeInsert(ctx, t, args)
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
		ret := &plan.SimpleQueryPlan{Stmt: stmt}
		ret.BindArgs(args)
		return ret, nil
	}

	var (
		shards   rule.DatabaseTables
		fullScan bool
		err      error
		vt       = ru.MustVTable(stmt.From[0].TableName().Suffix())
	)

	if shards, fullScan, err = (*Sharder)(ru).Shard(stmt.From[0].TableName(), stmt.Where, args...); err != nil {
		return nil, errors.Wrap(err, "calculate shards failed")
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	// return error if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, errors.WithStack(errDenyFullScan)
	}

	if shards.IsEmpty() {
		var (
			db0, tbl0 string
			ok        bool
		)
		if db0, tbl0, ok = vt.Topology().Render(0, 0); !ok {
			return nil, errors.Errorf("cannot compute minimal topology from '%s'", stmt.From[0].TableName().Suffix())
		}

		ret := &plan.SimpleQueryPlan{
			Stmt:     stmt,
			Database: db0,
			Tables:   []string{tbl0},
		}
		ret.BindArgs(args)

		return ret, nil
	}

	switch len(shards) {
	case 1:
		ret := &plan.SimpleQueryPlan{
			Stmt: stmt,
		}
		ret.BindArgs(args)
		for k, v := range shards {
			ret.Database = k
			ret.Tables = v
		}
		return ret, nil
	case 0:
		// init shards
		shards = rule.DatabaseTables{}
		// compute all tables
		topology := vt.Topology()
		topology.Each(func(dbIdx, tbIdx int) bool {
			if d, t, ok := topology.Render(dbIdx, tbIdx); ok {
				shards[d] = append(shards[d], t)
			}
			return true
		})
	}

	plans := make([]proto.Plan, 0, len(shards))
	for k, v := range shards {
		next := &plan.SimpleQueryPlan{
			Database: k,
			Tables:   v,
			Stmt:     stmt,
		}
		next.BindArgs(args)
		plans = append(plans, next)
	}

	unionPlan := &plan.UnionPlan{
		Plans: plans,
	}
	// TODO: order/groupBy/aggregate

	return unionPlan, nil
}

func (o optimizer) optimizeInsert(ctx context.Context, stmt *rast.InsertStatement, args []interface{}) (proto.Plan, error) {
	var (
		ru  = rcontext.Rule(ctx)
		ret = plan.NewSimpleInsertPlan()
	)

	ret.BindArgs(args)

	var (
		vt *rule.VTable
		ok bool
	)

	if vt, ok = ru.VTable(stmt.Table().Suffix()); !ok { // insert into non-sharding table
		ret.Put("", stmt)
		return ret, nil
	}

	// TODO: handle multiple shard keys.

	bingo := -1
	// check existing shard columns
	for i, col := range stmt.Columns() {
		if _, _, ok = vt.GetShardMetadata(col); ok {
			bingo = i
			break
		}
	}

	if bingo < 0 {
		return nil, errors.Wrap(errNoShardKeyFound, "failed to insert")
	}

	var (
		sharder = (*Sharder)(ru)
		left    = rast.ColumnNameExpressionAtom(make([]string, 1))
		filter  = &rast.PredicateExpressionNode{
			P: &rast.BinaryComparisonPredicateNode{
				Left: &rast.AtomPredicateNode{
					A: left,
				},
				Op: cmp.Ceq,
			},
		}
		slots = make(map[string]map[string][]int) // (db,table,valuesIndex)
	)

	// reset filter
	resetFilter := func(column string, value rast.ExpressionNode) {
		left[0] = column
		filter.P.(*rast.BinaryComparisonPredicateNode).Right = value.(*rast.PredicateExpressionNode).P
	}

	for i, values := range stmt.Values() {
		value := values[bingo]
		resetFilter(stmt.Columns()[bingo], value)

		shards, _, err := sharder.Shard(stmt.Table(), filter, args...)

		if err != nil {
			return nil, errors.WithStack(err)
		}

		if shards.Len() != 1 {
			return nil, errors.Wrap(errNoShardKeyFound, "failed to insert")
		}

		var (
			db    string
			table string
		)

		for k, v := range shards {
			db = k
			table = v[0]
			break
		}

		if _, ok = slots[db]; !ok {
			slots[db] = make(map[string][]int)
		}
		slots[db][table] = append(slots[db][table], i)
	}

	for db, slot := range slots {
		for table, indexes := range slot {
			// clone insert stmt without values
			newborn := rast.NewInsertStatement(rast.TableName{table}, stmt.Columns())
			newborn.SetFlag(stmt.Flag())
			newborn.SetDuplicatedUpdates(stmt.DuplicatedUpdates())

			// collect values with same table
			values := make([][]rast.ExpressionNode, 0, len(indexes))
			for _, i := range indexes {
				values = append(values, stmt.Values()[i])
			}
			newborn.SetValues(values)

			ret.Put(db, newborn)
		}
	}

	return ret, nil
}
