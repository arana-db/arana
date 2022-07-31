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

package dml

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/merge/aggregator"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
	"github.com/arana-db/arana/pkg/runtime/plan/dml"
	"github.com/arana-db/arana/pkg/transformer"
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	_bypass uint32 = 1 << iota //
	_supported
)

func init() {
	optimize.Register(ast.SQLTypeSelect, optimizeSelect)
}

func optimizeSelect(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.SelectStatement)

	// overwrite stmt limit x offset y. eg `select * from student offset 100 limit 5` will be
	// `select * from student offset 0 limit 100+5`
	originOffset, newLimit := overwriteLimit(stmt, &o.Args)
	if stmt.HasJoin() {
		return optimizeJoin(o, stmt)
	}
	flag := getSelectFlag(o.Rule, stmt)
	if flag&_supported == 0 {
		return nil, errors.Errorf("unsupported sql: %s", rcontext.SQL(ctx))
	}

	if flag&_bypass != 0 {
		if len(stmt.From) > 0 {
			err := rewriteSelectStatement(ctx, stmt, stmt.From[0].TableName().Suffix())
			if err != nil {
				return nil, err
			}
		}
		ret := &dml.SimpleQueryPlan{Stmt: stmt}
		ret.BindArgs(o.Args)
		return ret, nil
	}

	// --- SIMPLE QUERY BEGIN ---

	var (
		shards    rule.DatabaseTables
		fullScan  bool
		err       error
		vt        = o.Rule.MustVTable(stmt.From[0].TableName().Suffix())
		tableName = stmt.From[0].TableName()
	)
	if len(o.Hints) > 0 {
		if shards, err = optimize.Hints(tableName, o.Hints, o.Rule); err != nil {
			return nil, errors.Wrap(err, "calculate hints failed")
		}
	}

	if shards == nil {
		if shards, fullScan, err = (*optimize.Sharder)(o.Rule).Shard(tableName, stmt.Where, o.Args...); err != nil && fullScan == false {
			return nil, errors.Wrap(err, "calculate shards failed")
		}
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	// return error if full-scan is disabled
	if fullScan && (!vt.AllowFullScan() && !hint.Contains(hint.TypeFullScan, o.Hints)) {
		return nil, errors.WithStack(optimize.ErrDenyFullScan)
	}

	toSingle := func(db, tbl string) (proto.Plan, error) {
		_, tb0, _ := vt.Topology().Smallest()
		if err := rewriteSelectStatement(ctx, stmt, tb0); err != nil {
			return nil, err
		}
		ret := &dml.SimpleQueryPlan{
			Stmt:     stmt,
			Database: db,
			Tables:   []string{tbl},
		}
		ret.BindArgs(o.Args)

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

	_, tb, _ := vt.Topology().Smallest()
	if err = rewriteSelectStatement(ctx, stmt, tb); err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		analysis selectResult
		scanner  = newSelectScanner(stmt, o.Args)
	)

	if err = scanner.scan(&analysis); err != nil {
		return nil, errors.WithStack(err)
	}

	// Handle multiple shards

	if shards.IsFullScan() { // expand all shards if all shards matched
		shards = vt.Topology().Enumerate()
	}

	plans := make([]proto.Plan, 0, len(shards))
	for k, v := range shards {
		next := &dml.SimpleQueryPlan{
			Database: k,
			Tables:   v,
			Stmt:     stmt,
		}
		next.BindArgs(o.Args)
		plans = append(plans, next)
	}

	var tmpPlan proto.Plan
	tmpPlan = &dml.CompositePlan{
		Plans: plans,
	}

	// check if order-by exists
	if len(analysis.orders) > 0 {
		var (
			sb           strings.Builder
			orderByItems = make([]dataset.OrderByItem, 0, len(analysis.orders))
		)

		for _, it := range analysis.orders {
			var next dataset.OrderByItem
			next.Desc = it.Desc
			if alias := it.Alias(); len(alias) > 0 {
				next.Column = alias
			} else {
				switch prev := it.Prev().(type) {
				case *ast.SelectElementColumn:
					next.Column = prev.Suffix()
				default:
					if err = it.Restore(ast.RestoreWithoutAlias, &sb, nil); err != nil {
						return nil, errors.WithStack(err)
					}
					next.Column = sb.String()
					sb.Reset()
				}
			}
			orderByItems = append(orderByItems, next)
		}
		tmpPlan = &dml.OrderPlan{
			ParentPlan:   tmpPlan,
			OrderByItems: orderByItems,
		}
	}

	if stmt.GroupBy != nil {
		if tmpPlan, err = handleGroupBy(tmpPlan, stmt); err != nil {
			return nil, errors.WithStack(err)
		}
	} else {
		tmpPlan = &dml.AggregatePlan{
			Plan:       tmpPlan,
			Combiner:   transformer.NewCombinerManager(),
			AggrLoader: transformer.LoadAggrs(stmt.Select),
		}
	}

	if stmt.Limit != nil {
		tmpPlan = &dml.LimitPlan{
			ParentPlan:     tmpPlan,
			OriginOffset:   originOffset,
			OverwriteLimit: newLimit,
		}
	}

	// check & drop weak column
	var weaks []ast.SelectElement
	for i := range stmt.Select {
		if _, ok := stmt.Select[i].(ext.WeakMarker); ok {
			weaks = append(weaks, stmt.Select[i])
		}
	}
	if len(weaks) > 0 {
		tmpPlan = &dml.DropWeakPlan{
			Plan:     tmpPlan,
			WeakList: weaks,
		}
	}

	return tmpPlan, nil
}

// handleGroupBy exp: `select max(score) group by id order by name` will be convert to
// `select max(score), id group by id order by id, name`
func handleGroupBy(parentPlan proto.Plan, stmt *ast.SelectStatement) (proto.Plan, error) {
	groupPlan := &dml.GroupPlan{
		Plan:              parentPlan,
		AggItems:          aggregator.LoadAggs(stmt.Select),
		OriginColumnCount: len(stmt.Select),
	}

	var (
		items = stmt.GroupBy.Items
		lens  = len(items) + len(stmt.Select)

		selectItemsMap = make(map[string]ast.SelectElement)
		newSelectItems = make([]ast.SelectElement, 0, lens)

		orderItemMap    = make(map[string]*ast.OrderByItem)
		newOrderByItems = make([]*ast.OrderByItem, 0, lens)

		groupItems = make([]dataset.OrderByItem, 0, len(items))
	)

	for _, si := range stmt.Select {
		if sec, ok := si.(*ast.SelectElementColumn); ok {
			cn := sec.Name[len(sec.Name)-1]
			selectItemsMap[cn] = si
		}
	}

	for _, obi := range stmt.OrderBy {
		if cn, ok := obi.Expr.(*ast.ColumnNameExpressionAtom); ok {
			orderItemMap[cn.Suffix()] = obi
		}
	}

	newSelectItems = append(newSelectItems, stmt.Select...)
	for _, item := range items {
		if pen, ok := item.Expr().(*ast.PredicateExpressionNode); ok {
			if apn, ok := pen.P.(*ast.AtomPredicateNode); ok {
				if cn, ok := apn.Column(); ok {
					if _, ok := selectItemsMap[cn.Suffix()]; !ok {
						newSelectItems = append(newSelectItems, ast.NewSelectElementColumn(cn, cn.Suffix()))
					}
					if _, ok := orderItemMap[cn.Suffix()]; !ok {
						newOrderByItems = append(newOrderByItems, &ast.OrderByItem{
							Expr: cn,
							Desc: false,
						})
					}
					groupItems = append(groupItems, dataset.OrderByItem{
						Column: cn.Suffix(),
						Desc:   item.IsOrderDesc(),
					})
				}
			}
		}
	}

	if stmt.OrderBy != nil {
		newOrderByItems = append(newOrderByItems, stmt.OrderBy...)
	}

	stmt.Select = newSelectItems
	stmt.OrderBy = newOrderByItems
	groupPlan.GroupItems = groupItems

	return groupPlan, nil
}

// optimizeJoin ony support  a join b in one db
func optimizeJoin(o *optimize.Optimizer, stmt *ast.SelectStatement) (proto.Plan, error) {
	join := stmt.From[0].Source().(*ast.JoinNode)

	compute := func(tableSource *ast.TableSourceNode) (database, alias string, shardList []string, err error) {
		table := tableSource.TableName()
		if table == nil {
			err = errors.New("must table, not statement or join node")
			return
		}
		alias = tableSource.Alias
		database = table.Prefix()

		shards, err := o.ComputeShards(table, nil, o.Args)
		if err != nil {
			return
		}
		// table no shard
		if shards == nil {
			shardList = append(shardList, table.Suffix())
			return
		}
		// table  shard more than one db
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

	joinPan := &dml.SimpleJoinPlan{
		Left: &dml.JoinTable{
			Tables: shardLeft,
			Alias:  aliasLeft,
		},
		Join: join,
		Right: &dml.JoinTable{
			Tables: shardRight,
			Alias:  aliasRight,
		},
		Stmt: o.Stmt.(*ast.SelectStatement),
	}
	joinPan.BindArgs(o.Args)

	return joinPan, nil
}

func getSelectFlag(ru *rule.Rule, stmt *ast.SelectStatement) (flag uint32) {
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
		if !ru.Has(tn.Suffix()) {
			flag |= _bypass
		}
	case 0:
		flag |= _bypass
		flag |= _supported
	}
	return
}

func overwriteLimit(stmt *ast.SelectStatement, args *[]interface{}) (originOffset, overwriteLimit int64) {
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

func rewriteSelectStatement(ctx context.Context, stmt *ast.SelectStatement, tb string) error {
	// todo db 计算逻辑&tb shard 的计算逻辑
	starExpand := false
	if len(stmt.Select) == 1 {
		if _, ok := stmt.Select[0].(*ast.SelectElementAll); ok {
			starExpand = true
		}
	}

	if !starExpand {
		return nil
	}

	if len(tb) < 1 {
		tb = stmt.From[0].TableName().Suffix()
	}
	metadatas, err := proto.LoadSchemaLoader().Load(ctx, rcontext.Schema(ctx), []string{tb})
	if err != nil {
		return errors.WithStack(err)
	}
	metadata := metadatas[tb]
	if metadata == nil || len(metadata.ColumnNames) == 0 {
		return errors.Errorf("optimize: cannot get metadata of `%s`.`%s`", rcontext.Schema(ctx), tb)
	}

	selectElements := make([]ast.SelectElement, len(metadata.Columns))
	for i, column := range metadata.ColumnNames {
		selectElements[i] = ast.NewSelectElementColumn([]string{column}, "")
	}
	stmt.Select = selectElements

	return nil
}
