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
	"github.com/arana-db/arana/pkg/proto/schema_manager"
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

func (o optimizer) Optimize(ctx context.Context, conn proto.VConn, stmt ast.StmtNode, args ...interface{}) (plan proto.Plan, err error) {
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
	return o.doOptimize(ctx, conn, rstmt, args...)
}

func (o optimizer) doOptimize(ctx context.Context, conn proto.VConn, stmt rast.Statement, args ...interface{}) (proto.Plan, error) {
	switch t := stmt.(type) {
	case *rast.ShowDatabases:
		return o.optimizeShowDatabases(ctx, t, args)
	case *rast.SelectStatement:
		return o.optimizeSelect(ctx, conn, t, args)
	case *rast.InsertStatement:
		return o.optimizeInsert(ctx, conn, t, args)
	case *rast.DeleteStatement:
		return o.optimizeDelete(ctx, t, args)
	case *rast.UpdateStatement:
		return o.optimizeUpdate(ctx, conn, t, args)
	case *rast.TruncateStatement:
		return o.optimizeTruncate(ctx, t, args)
	case *rast.DropTableStatement:
		return o.optimizeDropTable(ctx, t, args)
	}

	//TODO implement all statements
	panic("implement me")
}

const (
	_bypass uint32 = 1 << iota
	_supported
)

func (o optimizer) optimizeDropTable(ctx context.Context, stmt *rast.DropTableStatement, args []interface{}) (proto.Plan, error) {
	ru := rcontext.Rule(ctx)
	//table shard
	var shards []rule.DatabaseTables
	//tables not shard
	noShardStmt := rast.NewDropTableStatement()
	for _, table := range stmt.Tables {
		shard, err := o.computeShards(ru, *table, nil, args)
		if err != nil {
			return nil, err
		}
		if shard == nil {
			noShardStmt.Tables = append(noShardStmt.Tables, table)
			continue
		}
		shards = append(shards, shard)
	}

	shardPlan := plan.NewDropTablePlan(stmt)
	shardPlan.BindArgs(args)
	shardPlan.SetShards(shards)

	if len(noShardStmt.Tables) == 0 {
		return shardPlan, nil
	}

	noShardPlan := plan.Transparent(noShardStmt, args)

	return &plan.UnionPlan{
		Plans: []proto.Plan{
			noShardPlan, shardPlan,
		},
	}, nil
}

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

func (o optimizer) optimizeShowDatabases(ctx context.Context, stmt *rast.ShowDatabases, args []interface{}) (proto.Plan, error) {
	ret := &plan.ShowDatabasesPlan{Stmt: stmt}
	ret.BindArgs(args)
	return ret, nil
}

func (o optimizer) optimizeSelect(ctx context.Context, conn proto.VConn, stmt *rast.SelectStatement, args []interface{}) (proto.Plan, error) {
	var ru *rule.Rule
	if ru = rcontext.Rule(ctx); ru == nil {
		return nil, errors.WithStack(errNoRuleFound)
	}

	flag := o.getSelectFlag(ctx, stmt)
	if flag&_supported == 0 {
		return nil, errors.Errorf("unsupported sql: %s", rcontext.SQL(ctx))
	}

	if flag&_bypass != 0 {
		if len(stmt.From) > 0 {
			err := o.rewriteStatement(ctx, conn, stmt, rcontext.DBGroup(ctx), stmt.From[0].TableName().Suffix())
			if err != nil {
				return nil, err
			}
		}
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

	// Go through first table if no shards matched.
	// For example:
	//    SELECT ... FROM xxx WHERE a > 8 and a < 4
	if shards.IsEmpty() {
		var (
			db0, tbl0 string
			ok        bool
		)
		if db0, tbl0, ok = vt.Topology().Render(0, 0); !ok {
			return nil, errors.Errorf("cannot compute minimal topology from '%s'", stmt.From[0].TableName().Suffix())
		}
		err := o.rewriteStatement(ctx, conn, stmt, db0, tbl0)
		if err != nil {
			return nil, err
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
		// TODO now only support single table
		err := o.rewriteStatement(ctx, conn, ret.Stmt, ret.Database, ret.Tables[0])
		if err != nil {
			return nil, err
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

	if len(plans) > 0 {
		tempPlan := plans[0].(*plan.SimpleQueryPlan)
		err := o.rewriteStatement(ctx, conn, stmt, tempPlan.Database, tempPlan.Tables[0])
		if err != nil {
			return nil, err
		}
	}
	unionPlan := &plan.UnionPlan{
		Plans: plans,
	}
	// TODO: order/groupBy/aggregate

	return unionPlan, nil
}

func (o optimizer) rewriteStatement(ctx context.Context, conn proto.VConn, stmt *rast.SelectStatement,
	db, tb string) error {
	// todo db 计算逻辑&tb shard 的计算逻辑
	var starExpand = false
	if len(stmt.Select) == 1 {
		if _, ok := stmt.Select[0].(*rast.SelectElementAll); ok {
			starExpand = true
		}
	}
	if starExpand {
		if len(tb) < 1 {
			tb = stmt.From[0].TableName().Suffix()
		}
		schemaLoader := &schema_manager.SimpleSchemaLoader{Schema: db}
		metaData := schemaLoader.Load(ctx, conn, []string{tb})[tb]
		if metaData == nil || len(metaData.ColumnNames) == 0 {
			return errors.Errorf("can not get metadata for db:%s and table:%s", db, tb)
		}
		selectElements := make([]rast.SelectElement, len(metaData.Columns))
		for i, column := range metaData.ColumnNames {
			selectElements[i] = rast.NewSelectElementColumn([]string{column}, "")
		}
		stmt.Select = selectElements
	}

	return nil
}

func (o optimizer) optimizeUpdate(ctx context.Context, conn proto.VConn, stmt *rast.UpdateStatement, args []interface{}) (proto.Plan, error) {
	var (
		ru    = rcontext.Rule(ctx)
		table = stmt.Table
		vt    *rule.VTable
		ok    bool
	)

	// non-sharding update
	if vt, ok = ru.VTable(table.Suffix()); !ok {
		ret := plan.NewUpdatePlan(stmt)
		ret.BindArgs(args)
		return ret, nil
	}

	var (
		shards   rule.DatabaseTables
		fullScan = true
		err      error
	)

	// compute shards
	if where := stmt.Where; where != nil {
		sharder := (*Sharder)(ru)
		if shards, fullScan, err = sharder.Shard(table, where, args...); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
	}

	// exit if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, errDenyFullScan
	}

	// must be empty shards (eg: update xxx set ... where 1 = 2 and uid = 1)
	if shards.IsEmpty() {
		return plan.AlwaysEmptyExecPlan{}, nil
	}

	// compute all sharding tables
	if shards.IsFullScan() {
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

	ret := plan.NewUpdatePlan(stmt)
	ret.BindArgs(args)
	ret.SetShards(shards)

	return ret, nil
}

func (o optimizer) optimizeInsert(ctx context.Context, conn proto.VConn, stmt *rast.InsertStatement, args []interface{}) (proto.Plan, error) {
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

func (o optimizer) optimizeDelete(ctx context.Context, stmt *rast.DeleteStatement, args []interface{}) (proto.Plan, error) {
	ru := rcontext.Rule(ctx)
	shards, err := o.computeShards(ru, stmt.Table, stmt.Where, args)
	if err != nil {
		return nil, errors.Wrap(err, "failed to optimize DELETE statement")
	}

	// TODO: delete from a child sharding-table directly

	if shards == nil {
		return plan.Transparent(stmt, args), nil
	}

	ret := plan.NewSimpleDeletePlan(stmt)
	ret.BindArgs(args)
	ret.SetShards(shards)

	return ret, nil
}

func (o optimizer) optimizeTruncate(ctx context.Context, stmt *rast.TruncateStatement, args []interface{}) (proto.Plan, error) {
	ru := rcontext.Rule(ctx)
	shards, err := o.computeShards(ru, stmt.Table, nil, args)
	if err != nil {
		return nil, errors.Wrap(err, "failed to optimize TRUNCATE statement")
	}

	if shards == nil {
		return plan.Transparent(stmt, args), nil
	}

	ret := plan.NewTruncatePlan(stmt)
	ret.BindArgs(args)
	ret.SetShards(shards)

	return ret, nil
}

func (o optimizer) computeShards(ru *rule.Rule, table rast.TableName, where rast.ExpressionNode, args []interface{}) (rule.DatabaseTables, error) {
	vt, ok := ru.VTable(table.Suffix())
	if !ok {
		return nil, nil
	}

	shards, fullScan, err := (*Sharder)(ru).Shard(table, where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "calculate shards failed")
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	// return error if full-scan is disabled
	if fullScan && !vt.AllowFullScan() {
		return nil, errors.WithStack(errDenyFullScan)
	}

	if shards.IsEmpty() {
		return shards, nil
	}

	if len(shards) == 0 {
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

	return shards, nil
}
