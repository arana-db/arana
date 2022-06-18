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
	"github.com/arana-db/arana/pkg/runtime"
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
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/transformer"
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
	return optimizer{
		schemaLoader: schema_manager.NewSimpleSchemaLoader(),
	}
}

type optimizer struct {
	schemaLoader proto.SchemaLoader
}

func (o *optimizer) SetSchemaLoader(schemaLoader proto.SchemaLoader) {
	o.schemaLoader = schemaLoader
}

func (o *optimizer) SchemaLoader() proto.SchemaLoader {
	return o.schemaLoader
}

func (o optimizer) Optimize(ctx context.Context, conn proto.VConn, stmt ast.StmtNode, args ...interface{}) (plan proto.Plan, err error) {
	ctx, span := runtime.Tracer.Start(ctx, "Optimize")
	defer func() {
		span.End()
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
	case *rast.InsertSelectStatement:
		return o.optimizeInsertSelect(ctx, conn, t, args)
	case *rast.DeleteStatement:
		return o.optimizeDelete(ctx, t, args)
	case *rast.UpdateStatement:
		return o.optimizeUpdate(ctx, conn, t, args)
	case *rast.ShowOpenTables:
		return o.optimizeShowOpenTables(ctx, t, args)
	case *rast.ShowTables:
		return o.optimizeShowTables(ctx, t, args)
	case *rast.ShowIndex:
		return o.optimizeShowIndex(ctx, t, args)
	case *rast.TruncateStatement:
		return o.optimizeTruncate(ctx, t, args)
	case *rast.DropTableStatement:
		return o.optimizeDropTable(ctx, t, args)
	case *rast.ShowVariables:
		return o.optimizeShowVariables(ctx, t, args)
	case *rast.DescribeStatement:
		return o.optimizeDescribeStatement(ctx, t, args)
	case *rast.AlterTableStatement:
		return o.optimizeAlterTable(ctx, t, args)
	case *rast.DropIndexStatement:
		return o.optimizeDropIndex(ctx, t, args)
	}

	//TODO implement all statements
	panic("implement me")
}

const (
	_bypass uint32 = 1 << iota
	_supported
)

func (o optimizer) optimizeDropIndex(ctx context.Context, stmt *rast.DropIndexStatement, args []interface{}) (proto.Plan, error) {
	ru := rcontext.Rule(ctx)
	//table shard

	shard, err := o.computeShards(ru, stmt.Table, nil, args)
	if err != nil {
		return nil, err
	}
	if len(shard) == 0 {
		return plan.Transparent(stmt, args), nil
	}

	shardPlan := plan.NewDropIndexPlan(stmt)
	shardPlan.SetShard(shard)
	shardPlan.BindArgs(args)
	return shardPlan, nil
}

func (o optimizer) optimizeAlterTable(ctx context.Context, stmt *rast.AlterTableStatement, args []interface{}) (proto.Plan, error) {
	var (
		ret   = plan.NewAlterTablePlan(stmt)
		ru    = rcontext.Rule(ctx)
		table = stmt.Table
		vt    *rule.VTable
		ok    bool
	)
	ret.BindArgs(args)

	// non-sharding update
	if vt, ok = ru.VTable(table.Suffix()); !ok {
		return ret, nil
	}

	//TODO alter table table or column to new name , should update sharding info

	// exit if full-scan is disabled
	if !vt.AllowFullScan() {
		return nil, errDenyFullScan
	}

	// sharding
	shards := rule.DatabaseTables{}
	topology := vt.Topology()
	topology.Each(func(dbIdx, tbIdx int) bool {
		if d, t, ok := topology.Render(dbIdx, tbIdx); ok {
			shards[d] = append(shards[d], t)
		}
		return true
	})
	ret.Shards = shards
	return ret, nil
}

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

func (o optimizer) overwriteLimit(stmt *rast.SelectStatement, args *[]interface{}) (originOffset, overwriteLimit int64) {
	if stmt == nil || stmt.Limit == nil {
		return 0, 0
	}

	offset := stmt.Limit.Offset()
	limit := stmt.Limit.Limit()

	// SELECT * FROM student where uid = ? limit ? offset ?
	var offsetIndex int64
	var limitIndex int64

	if stmt.Limit.IsOffsetVar() {
		offsetIndex = offset
		offset = (*args)[offsetIndex].(int64)

		if !stmt.Limit.IsLimitVar() {
			limit = stmt.Limit.Limit()
			*args = append(*args, limit)
			limitIndex = int64(len(*args) - 1)
		}
	}
	originOffset = offset

	if stmt.Limit.IsLimitVar() {
		limitIndex = limit
		limit = (*args)[limitIndex].(int64)

		if !stmt.Limit.IsOffsetVar() {
			*args = append(*args, int64(0))
			offsetIndex = int64(len(*args) - 1)
		}
	}

	if stmt.Limit.IsLimitVar() || stmt.Limit.IsOffsetVar() {
		if !stmt.Limit.IsLimitVar() {
			stmt.Limit.SetLimitVar()
			stmt.Limit.SetLimit(limitIndex)
		}
		if !stmt.Limit.IsOffsetVar() {
			stmt.Limit.SetOffsetVar()
			stmt.Limit.SetOffset(offsetIndex)
		}

		newLimitVar := limit + offset
		overwriteLimit = newLimitVar
		(*args)[limitIndex] = newLimitVar
		(*args)[offsetIndex] = int64(0)
		return
	}

	stmt.Limit.SetOffset(0)
	stmt.Limit.SetLimit(offset + limit)
	overwriteLimit = offset + limit
	return
}

func (o optimizer) optimizeSelect(ctx context.Context, conn proto.VConn, stmt *rast.SelectStatement, args []interface{}) (proto.Plan, error) {
	var ru *rule.Rule
	if ru = rcontext.Rule(ctx); ru == nil {
		return nil, errors.WithStack(errNoRuleFound)
	}

	// overwrite stmt limit x offset y. eg `select * from student offset 100 limit 5` will be
	// `select * from student offset 0 limit 100+5`
	originOffset, overwriteLimit := o.overwriteLimit(stmt, &args)
	if stmt.HasJoin() {
		return o.optimizeJoin(ctx, conn, stmt, args)
	}
	flag := o.getSelectFlag(ctx, stmt)
	if flag&_supported == 0 {
		return nil, errors.Errorf("unsupported sql: %s", rcontext.SQL(ctx))
	}

	if flag&_bypass != 0 {
		if len(stmt.From) > 0 {
			err := o.rewriteSelectStatement(ctx, conn, stmt, rcontext.DBGroup(ctx), stmt.From[0].TableName().Suffix())
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

	toSingle := func(db, tbl string) (proto.Plan, error) {
		if err := o.rewriteSelectStatement(ctx, conn, stmt, db, tbl); err != nil {
			return nil, err
		}
		ret := &plan.SimpleQueryPlan{
			Stmt:     stmt,
			Database: db,
			Tables:   []string{tbl},
		}
		ret.BindArgs(args)

		return ret, nil
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

		return toSingle(db0, tbl0)
	}

	// Handle single shard
	if shards.Len() == 1 {
		var db, tbl string
		for k, v := range shards {
			db = k
			tbl = v[0]
		}
		return toSingle(db, tbl)
	}

	// Handle multiple shards

	if shards.IsFullScan() { // expand all shards if all shards matched
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
		if err = o.rewriteSelectStatement(ctx, conn, stmt, tempPlan.Database, tempPlan.Tables[0]); err != nil {
			return nil, err
		}
	}

	var tmpPlan proto.Plan
	tmpPlan = &plan.UnionPlan{
		Plans: plans,
	}

	if stmt.Limit != nil {
		tmpPlan = &plan.LimitPlan{
			ParentPlan:     tmpPlan,
			OriginOffset:   originOffset,
			OverwriteLimit: overwriteLimit,
		}
	}

	// TODO: order/groupBy/aggregate
	aggregate := &plan.AggregatePlan{
		Plan:       tmpPlan,
		Combiner:   transformer.NewCombinerManager(),
		AggrLoader: transformer.LoadAggrs(stmt.Select),
	}

	return aggregate, nil
}

//optimizeJoin ony support  a join b in one db
func (o optimizer) optimizeJoin(ctx context.Context, conn proto.VConn, stmt *rast.SelectStatement, args []interface{}) (proto.Plan, error) {

	var ru *rule.Rule
	if ru = rcontext.Rule(ctx); ru == nil {
		return nil, errors.WithStack(errNoRuleFound)
	}

	join := stmt.From[0].Source().(*rast.JoinNode)

	compute := func(tableSource *rast.TableSourceNode) (database, alias string, shardList []string, err error) {
		table := tableSource.TableName()
		if table == nil {
			err = errors.New("must table, not statement or join node")
			return
		}
		alias = tableSource.Alias()
		database = table.Prefix()

		shards, err := o.computeShards(ru, table, nil, args)
		if err != nil {
			return
		}
		//table no shard
		if shards == nil {
			shardList = append(shardList, table.Suffix())
			return
		}
		//table  shard more than one db
		if len(shards) > 1 {
			err = errors.New("not support more than one db")
			return
		}

		for k, v := range shards {
			database = k
			shardList = v
		}

		if alias == "" {
			alias = table.Suffix()
		}

		return
	}

	dbLeft, aliasLeft, shardLeft, err := compute(join.Left)
	if err != nil {
		return nil, err
	}
	dbRight, aliasRight, shardRight, err := compute(join.Right)

	if err != nil {
		return nil, err
	}

	if dbLeft != "" && dbRight != "" && dbLeft != dbRight {
		return nil, errors.New("not support more than one db")
	}

	joinPan := &plan.SimpleJoinPlan{
		Left: &plan.JoinTable{
			Tables: shardLeft,
			Alias:  aliasLeft,
		},
		Join: join,
		Right: &plan.JoinTable{
			Tables: shardRight,
			Alias:  aliasRight,
		},
		Stmt: stmt,
	}
	joinPan.BindArgs(args)

	return joinPan, nil
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

	//check update sharding key
	for _, element := range stmt.Updated {
		if _, _, ok := vt.GetShardMetadata(element.Column.Suffix()); ok {
			return nil, errors.New("do not support update sharding key")
		}
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

	//check on duplicated key update
	for _, upd := range stmt.DuplicatedUpdates() {
		if upd.Column.Suffix() == stmt.Columns()[bingo] {
			return nil, errors.New("do not support update sharding key")
		}
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

			o.rewriteInsertStatement(ctx, conn, newborn, db, table)
			ret.Put(db, newborn)
		}
	}

	return ret, nil
}

func (o optimizer) optimizeInsertSelect(ctx context.Context, conn proto.VConn, stmt *rast.InsertSelectStatement, args []interface{}) (proto.Plan, error) {
	var (
		ru  = rcontext.Rule(ctx)
		ret = plan.NewInsertSelectPlan()
	)

	ret.BindArgs(args)

	if _, ok := ru.VTable(stmt.Table().Suffix()); !ok { // insert into non-sharding table
		ret.Batch[""] = stmt
		return ret, nil
	}

	// TODO: handle shard keys.

	return nil, errors.New("not support insert-select into sharding table")
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

func (o optimizer) optimizeShowOpenTables(ctx context.Context, stmt *rast.ShowOpenTables, args []interface{}) (proto.Plan, error) {
	var invertedIndex map[string]string
	for logicalTable, v := range rcontext.Rule(ctx).VTables() {
		t := v.Topology()
		t.Each(func(x, y int) bool {
			if _, phyTable, ok := t.Render(x, y); ok {
				if invertedIndex == nil {
					invertedIndex = make(map[string]string)
				}
				invertedIndex[phyTable] = logicalTable
			}
			return true
		})
	}

	clusters := security.DefaultTenantManager().GetClusters(rcontext.Tenant(ctx))
	plans := make([]proto.Plan, 0, len(clusters))
	for _, cluster := range clusters {
		ns := namespace.Load(cluster)
		// 配置里原子库 都需要执行一次
		groups := ns.DBGroups()
		for i := 0; i < len(groups); i++ {
			ret := plan.NewShowOpenTablesPlan(stmt)
			ret.BindArgs(args)
			ret.SetInvertedShards(invertedIndex)
			ret.SetDatabase(groups[i])
			plans = append(plans, ret)
		}
	}

	unionPlan := &plan.UnionPlan{
		Plans: plans,
	}

	aggregate := &plan.AggregatePlan{
		Plan:       unionPlan,
		Combiner:   transformer.NewCombinerManager(),
		AggrLoader: transformer.LoadAggrs(nil),
	}

	return aggregate, nil
}

func (o optimizer) optimizeShowTables(ctx context.Context, stmt *rast.ShowTables, args []interface{}) (proto.Plan, error) {
	var invertedIndex map[string]string
	for logicalTable, v := range rcontext.Rule(ctx).VTables() {
		t := v.Topology()
		t.Each(func(x, y int) bool {
			if _, phyTable, ok := t.Render(x, y); ok {
				if invertedIndex == nil {
					invertedIndex = make(map[string]string)
				}
				invertedIndex[phyTable] = logicalTable
			}
			return true
		})
	}

	ret := plan.NewShowTablesPlan(stmt)
	ret.BindArgs(args)
	ret.SetInvertedShards(invertedIndex)
	return ret, nil
}

func (o optimizer) optimizeShowIndex(_ context.Context, stmt *rast.ShowIndex, args []interface{}) (proto.Plan, error) {
	ret := &plan.ShowIndexPlan{Stmt: stmt}
	ret.BindArgs(args)
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

func (o optimizer) optimizeShowVariables(ctx context.Context, stmt *rast.ShowVariables, args []interface{}) (proto.Plan, error) {
	ret := plan.NewShowVariablesPlan(stmt)
	ret.BindArgs(args)
	return ret, nil
}

func (o optimizer) optimizeDescribeStatement(ctx context.Context, stmt *rast.DescribeStatement, args []interface{}) (proto.Plan, error) {
	vts := rcontext.Rule(ctx).VTables()
	vtName := []string(stmt.Table)[0]
	ret := plan.NewDescribePlan(stmt)
	ret.BindArgs(args)

	if vTable, ok := vts[vtName]; ok {
		shards := rule.DatabaseTables{}
		// compute all tables
		topology := vTable.Topology()
		topology.Each(func(dbIdx, tbIdx int) bool {
			if d, t, ok := topology.Render(dbIdx, tbIdx); ok {
				shards[d] = append(shards[d], t)
			}
			return true
		})
		dbName, tblName := shards.Smallest()
		ret.Database = dbName
		ret.Table = tblName
		ret.Column = stmt.Column
	}

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

func (o optimizer) rewriteSelectStatement(ctx context.Context, conn proto.VConn, stmt *rast.SelectStatement,
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
		metaData := o.schemaLoader.Load(ctx, conn, db, []string{tb})[tb]
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

func (o optimizer) rewriteInsertStatement(ctx context.Context, conn proto.VConn, stmt *rast.InsertStatement,
	db, tb string) error {
	metaData := o.schemaLoader.Load(ctx, conn, db, []string{tb})[tb]
	if metaData == nil || len(metaData.ColumnNames) == 0 {
		return errors.Errorf("can not get metadata for db:%s and table:%s", db, tb)
	}

	if len(metaData.ColumnNames) == len(stmt.Columns()) {
		// User had explicitly specified every value
		return nil
	}
	columnsMetadata := metaData.Columns

	for _, colName := range stmt.Columns() {
		if columnsMetadata[colName].PrimaryKey && columnsMetadata[colName].Generated {
			// User had explicitly specified auto-generated primary key column
			return nil
		}
	}

	pkColName := ""
	for name, column := range columnsMetadata {
		if column.PrimaryKey && column.Generated {
			pkColName = name
			break
		}
	}
	if len(pkColName) < 1 {
		// There's no auto-generated primary key column
		return nil
	}

	// TODO rewrite columns and add distributed primary key
	//stmt.SetColumns(append(stmt.Columns(), pkColName))
	// append value of distributed primary key
	//newValues := stmt.Values()
	//for _, newValue := range newValues {
	//	newValue = append(newValue, )
	//}
	return nil
}
