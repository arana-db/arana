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
	mysql "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/merge/aggregator"
	mysqlErrors "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/misc/extvalue"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
	"github.com/arana-db/arana/pkg/runtime/plan/dml"
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
	enableLocalMathComputation := ctx.Value(proto.ContextKeyEnableLocalComputation{}).(bool)
	if enableLocalMathComputation && len(stmt.From) == 0 {
		isLocalFlag := true
		var columnList []string
		var valueList []proto.Value
		for i := range stmt.Select {
			switch selectItem := stmt.Select[i].(type) {
			case *ast.SelectElementExpr:
				var nodeInner *ast.PredicateExpressionNode
				calculateNode := selectItem.Expression()
				if _, ok := calculateNode.(*ast.PredicateExpressionNode); ok {
					nodeInner = calculateNode.(*ast.PredicateExpressionNode)
				} else {
					isLocalFlag = false
					break
				}
				calculateRes, errtmp := extvalue.Compute(ctx, nodeInner.P)
				if errtmp != nil {
					isLocalFlag = false
					break
				}

				valueList = append(valueList, calculateRes)
				columnList = append(columnList, stmt.Select[i].DisplayName())
			case *ast.SelectElementFunction:
				var nodeF ast.Node
				calculateNode := selectItem.Function()
				if _, ok := calculateNode.(*ast.Function); ok {
					nodeF = calculateNode.(*ast.Function)
				} else {
					isLocalFlag = false
					break
				}
				calculateRes, errTmp := extvalue.Compute(ctx, nodeF)
				if errTmp != nil {
					isLocalFlag = false
					break
				}
				valueList = append(valueList, calculateRes)
				columnList = append(columnList, stmt.Select[i].DisplayName())

			}
		}
		if isLocalFlag {

			ret := &dml.LocalSelectPlan{
				Stmt:       stmt,
				Result:     valueList,
				ColumnList: columnList,
			}
			ret.BindArgs(o.Args)

			return ret, nil
		}

	}
	if stmt.HasJoin() {
		return optimizeJoin(ctx, o, stmt)
	}

	// overwrite stmt limit x offset y. eg `select * from student offset 100 limit 5` will be
	// `select * from student offset 0 limit 100+5`
	originOffset, newLimit := overwriteLimit(stmt, &o.Args)

	flag := getSelectFlag(o.Rule, stmt)
	if flag&_supported == 0 {
		return nil, errors.Errorf("unsupported sql: %s", rcontext.SQL(ctx))
	}

	if flag&_bypass != 0 {
		if len(stmt.From) > 0 {
			err := rewriteSelectStatement(ctx, stmt, o)
			if err != nil {
				return nil, err
			}
		}

		ret := &dml.SimpleQueryPlan{Stmt: stmt}
		ret.BindArgs(o.Args)

		normalizedFields := make([]string, 0, len(stmt.Select))
		for i := range stmt.Select {
			normalizedFields = append(normalizedFields, stmt.Select[i].DisplayName())
		}

		return &dml.RenamePlan{
			Plan:       ret,
			RenameList: normalizedFields,
		}, nil
	}

	// --- SIMPLE QUERY BEGIN ---

	var (
		shards    rule.DatabaseTables
		fullScan  bool
		err       error
		tableName = stmt.From[0].Source.(ast.TableName)
		vt        = o.Rule.MustVTable(tableName.Suffix())
	)
	if len(o.Hints) > 0 {
		if shards, err = optimize.Hints(tableName, o.Hints, o.Rule); err != nil {
			return nil, errors.Wrap(err, "calculate hints failed")
		}
	}

	if shards == nil {
		if shards, err = optimize.NewXSharder(ctx, o.Rule, o.Args).SimpleShard(tableName, stmt.Where); err != nil {
			return nil, errors.WithStack(err)
		}
		fullScan = shards == nil
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)
	// return error if full-scan is disabled
	if fullScan && (!vt.AllowFullScan() && !hint.Contains(hint.TypeFullScan, o.Hints)) {
		return nil, errors.WithStack(optimize.ErrDenyFullScan)
	}

	toSingle := func(db, tbl string) (proto.Plan, error) {
		if err := rewriteSelectStatement(ctx, stmt, o); err != nil {
			return nil, err
		}
		ret := &dml.SimpleQueryPlan{
			Stmt:     stmt,
			Database: db,
			Tables:   []string{tbl},
		}
		ret.BindArgs(o.Args)

		normalizedFields := make([]string, 0, len(stmt.Select))
		for i := range stmt.Select {
			normalizedFields = append(normalizedFields, stmt.Select[i].DisplayName())
		}

		return &dml.RenamePlan{
			Plan:       ret,
			RenameList: normalizedFields,
		}, nil
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
			return nil, errors.Errorf("cannot compute minimal topology from '%s'", stmt.From[0].Source.(ast.TableName).Suffix())
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

	if err = rewriteSelectStatement(ctx, stmt, o); err != nil {
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
	} else if analysis.hasAggregate {
		tmpPlan = &dml.AggregatePlan{
			Plan:   tmpPlan,
			Fields: stmt.Select,
		}
	}

	if stmt.Limit != nil {
		tmpPlan = &dml.LimitPlan{
			ParentPlan:     tmpPlan,
			OriginOffset:   originOffset,
			OverwriteLimit: newLimit,
		}
	}

	if analysis.hasMapping {
		tmpPlan = &dml.MappingPlan{
			Plan:   tmpPlan,
			Fields: stmt.Select,
		}
	}

	// check & drop weak column
	if analysis.hasWeak {
		var weaks []*ext.WeakSelectElement
		for i := range stmt.Select {
			switch next := stmt.Select[i].(type) {
			case *ext.WeakSelectElement:
				weaks = append(weaks, next)
			}
		}
		if len(weaks) > 0 {
			tmpPlan = &dml.DropWeakPlan{
				Plan:     tmpPlan,
				WeakList: weaks,
			}
		}
	}

	// FIXME: tuning, avoid rename everytime.

	// Rename return fields as normalized:
	// For the query of "SELECT foo+1, avg(score) FROM xxx WHERE ...", will return columns:
	//   BEFORE: | `foo`+1 | AVG(`score`) |
	//      NOW: | foo+1 | avg(score) |
	tmpPlan = &dml.RenamePlan{
		Plan:       tmpPlan,
		RenameList: analysis.normalizedFields,
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

// optimizeJoin ony support  a join b in one db.
// DEPRECATED: reimplement in the future
func optimizeJoin(ctx context.Context, o *optimize.Optimizer, stmt *ast.SelectStatement) (proto.Plan, error) {
	compute := func(tableSource *ast.TableSourceItem) (database, alias string, table ast.TableName, shards rule.DatabaseTables, err error) {
		table = tableSource.Source.(ast.TableName)
		if table == nil {
			err = errors.New("must table, not statement or join node")
			return
		}
		alias = tableSource.Alias
		database = table.Prefix()

		if alias == "" {
			alias = table.Suffix()
		}

		shards, err = o.ComputeShards(ctx, table, nil, o.Args)
		if err != nil {
			return
		}
		return
	}

	from := stmt.From[0]
	dbLeft, aliasLeft, tableLeft, shardsLeft, err := compute(&from.TableSourceItem)
	if err != nil {
		return nil, err
	}

	join := from.Joins[0]
	dbRight, aliasRight, tableRight, shardsRight, err := compute(join.Target)
	if err != nil {
		return nil, err
	}

	// one db
	if dbLeft == dbRight && shardsLeft == nil && shardsRight == nil {
		joinPan := &dml.SimpleJoinPlan{
			Left: &dml.JoinTable{
				Tables: tableLeft,
				Alias:  aliasLeft,
			},
			Join: from.Joins[0],
			Right: &dml.JoinTable{
				Tables: tableRight,
				Alias:  aliasRight,
			},
			Stmt: o.Stmt.(*ast.SelectStatement),
		}
		joinPan.BindArgs(o.Args)
		return joinPan, nil
	}

	//multiple shards & do hash join
	hashJoinPlan := &dml.HashJoinPlan{
		Stmt: stmt,
	}

	onExpression, ok := from.Joins[0].On.(*ast.PredicateExpressionNode).P.(*ast.BinaryComparisonPredicateNode)
	// todo support more 'ON' condition  ast.LogicalExpressionNode
	if !ok {
		return nil, errors.New("not support more than one 'ON' condition")
	}

	onLeft := onExpression.Left.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
	onRight := onExpression.Right.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	leftKey := ""
	if onLeft.Prefix() == aliasLeft {
		leftKey = onLeft.Suffix()
	}

	rightKey := ""
	if onRight.Prefix() == aliasRight {
		rightKey = onRight.Suffix()
	}

	if len(leftKey) == 0 || len(rightKey) == 0 {
		return nil, errors.Errorf("not found buildKey or probeKey")
	}

	rewriteToSingle := func(tableSource ast.TableSourceItem, shards map[string][]string, onKey string) (proto.Plan, error) {
		selectStmt := &ast.SelectStatement{
			Select: stmt.Select,
			From: ast.FromNode{
				&ast.TableSourceNode{
					TableSourceItem: tableSource,
				},
			},
		}
		table := tableSource.Source.(ast.TableName)
		actualTb := table.Suffix()
		aliasTb := tableSource.Alias

		tb0 := actualTb
		if shards != nil {
			vt := o.Rule.MustVTable(tb0)
			_, tb0, _ = vt.Topology().Smallest()
		}
		if _, ok = stmt.Select[0].(*ast.SelectElementAll); !ok && len(stmt.Select) > 1 {
			metadata, err := loadMetadataByTable(ctx, tb0)
			if err != nil {
				return nil, err
			}

			selectColumn := selectStmt.Select
			var selectElements []ast.SelectElement
			for _, element := range selectColumn {
				e, ok := element.(*ast.SelectElementColumn)
				if ok {
					columnsMap := metadata.Columns
					ColumnMeta, exist := columnsMap[e.Suffix()]
					if (aliasTb == e.Prefix() || actualTb == e.Prefix()) && exist {
						selectElements = append(selectElements, ast.NewSelectElementColumn([]string{ColumnMeta.Name}, ""))
					}
				}
			}
			selectElements = append(selectElements, ast.NewSelectElementColumn([]string{onKey}, ""))
			selectStmt.Select = selectElements
		}

		if stmt.Where != nil {
			selectStmt.Where = stmt.Where.Clone()
			err := filterWhereByTable(ctx, selectStmt.Where, tb0, aliasTb)
			if err != nil {
				return nil, err
			}
		}

		optimizer := &optimize.Optimizer{
			Rule: o.Rule,
			Stmt: selectStmt,
		}
		if _, ok = selectStmt.Select[0].(*ast.SelectElementAll); ok && len(selectStmt.Select) == 1 {
			if err = rewriteSelectStatement(ctx, selectStmt, optimizer); err != nil {
				return nil, err
			}

			selectStmt.Select = append(selectStmt.Select, ast.NewSelectElementColumn([]string{onKey}, ""))
		}

		plan, err := optimizeSelect(ctx, optimizer)
		if err != nil {
			return nil, err
		}
		return plan, nil
	}

	leftPlan, err := rewriteToSingle(from.TableSourceItem, shardsLeft, leftKey)
	if err != nil {
		return nil, err
	}

	rightPlan, err := rewriteToSingle(*from.Joins[0].Target, shardsRight, rightKey)
	if err != nil {
		return nil, err
	}

	setPlan := func(plan *dml.HashJoinPlan, buildPlan, probePlan proto.Plan, buildKey, probeKey string) {
		plan.BuildKey = buildKey
		plan.ProbeKey = probeKey
		plan.BuildPlan = buildPlan
		plan.ProbePlan = probePlan
	}

	if join.Typ == ast.InnerJoin {
		setPlan(hashJoinPlan, leftPlan, rightPlan, leftKey, rightKey)
		hashJoinPlan.IsFilterProbeRow = true
	} else {
		hashJoinPlan.IsFilterProbeRow = false
		if join.Typ == ast.LeftJoin {
			hashJoinPlan.IsReversedColumn = true
			setPlan(hashJoinPlan, rightPlan, leftPlan, rightKey, leftKey)
		} else if join.Typ == ast.RightJoin {
			setPlan(hashJoinPlan, leftPlan, rightPlan, leftKey, rightKey)
		} else {
			return nil, errors.New("not support Join Type")
		}
	}

	var tmpPlan proto.Plan
	tmpPlan = hashJoinPlan

	var (
		analysis selectResult
		scanner  = newSelectScanner(stmt, o.Args)
	)

	if err = rewriteSelectStatement(ctx, stmt, o); err != nil {
		return nil, errors.WithStack(err)
	}

	if err = scanner.scan(&analysis); err != nil {
		return nil, errors.WithStack(err)
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
	} else if analysis.hasAggregate {
		tmpPlan = &dml.AggregatePlan{
			Plan:   tmpPlan,
			Fields: stmt.Select,
		}
	}

	if stmt.Limit != nil {
		// overwrite stmt limit x offset y. eg `select * from student offset 100 limit 5` will be
		// `select * from student offset 0 limit 100+5`
		originOffset, newLimit := overwriteLimit(stmt, &o.Args)
		tmpPlan = &dml.LimitPlan{
			ParentPlan:     tmpPlan,
			OriginOffset:   originOffset,
			OverwriteLimit: newLimit,
		}
	}

	if analysis.hasMapping {
		tmpPlan = &dml.MappingPlan{
			Plan:   tmpPlan,
			Fields: stmt.Select,
		}
	}

	tmpPlan = &dml.RenamePlan{
		Plan:       tmpPlan,
		RenameList: analysis.normalizedFields,
	}
	return tmpPlan, nil
}

func getSelectFlag(ru *rule.Rule, stmt *ast.SelectStatement) (flag uint32) {
	switch len(stmt.From) {
	case 1:
		from := stmt.From[0]
		tn := from.Source.(ast.TableName)

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

func overwriteLimit(stmt *ast.SelectStatement, args *[]proto.Value) (originOffset, overwriteLimit int64) {
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
		offset, _ = (*args)[offsetIndex].Int64()

		if !stmt.Limit.IsLimitVar() {
			limit = stmt.Limit.Limit()
			*args = append(*args, proto.NewValueInt64(limit))
			limitIndex = int64(len(*args) - 1)
		}
	}
	originOffset = offset

	if stmt.Limit.IsLimitVar() {
		limitIndex = limit
		limit, _ = (*args)[limitIndex].Int64()

		if !stmt.Limit.IsOffsetVar() {
			*args = append(*args, proto.NewValueInt64(0))
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
		(*args)[limitIndex] = proto.NewValueInt64(newLimitVar)
		(*args)[offsetIndex] = proto.NewValueInt64(0)
		return
	}

	stmt.Limit.SetOffset(0)
	stmt.Limit.SetLimit(offset + limit)
	overwriteLimit = offset + limit
	return
}

func rewriteSelectStatement(ctx context.Context, stmt *ast.SelectStatement, o *optimize.Optimizer) error {
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

	tbs := []ast.TableName{stmt.From[0].Source.(ast.TableName)}
	for _, join := range stmt.From[0].Joins {
		joinTable := join.Target.Source.(ast.TableName)
		tbs = append(tbs, joinTable)
	}

	selectExpandElements := make([]ast.SelectElement, 0)
	for _, t := range tbs {
		shards, err := o.ComputeShards(ctx, t, nil, o.Args)
		if err != nil {
			return errors.WithStack(err)
		}

		tb0 := t.Suffix()
		if shards != nil {
			vt := o.Rule.MustVTable(tb0)
			_, tb0, _ = vt.Topology().Smallest()
		}

		metadata, err := loadMetadataByTable(ctx, tb0)
		if err != nil {
			return errors.WithStack(err)
		}

		for _, column := range metadata.ColumnNames {
			selectExpandElements = append(selectExpandElements, ast.NewSelectElementColumn([]string{column}, ""))
		}
	}
	stmt.Select = selectExpandElements
	return nil
}

func loadMetadataByTable(ctx context.Context, tb string) (*proto.TableMetadata, error) {
	metadatas, err := proto.LoadSchemaLoader().Load(ctx, rcontext.Schema(ctx), []string{tb})
	if err != nil {
		if strings.Contains(err.Error(), "Table doesn't exist") {
			return nil, mysqlErrors.NewSQLError(mysql.ERNoSuchTable, mysql.SSNoTableSelected, "Table '%s' doesn't exist", tb)
		}
		return nil, errors.WithStack(err)
	}

	metadata := metadatas[tb]
	if metadata == nil || len(metadata.ColumnNames) == 0 {
		return nil, errors.Errorf("optimize: cannot get metadata of `%s`.`%s`", rcontext.Schema(ctx), tb)
	}
	return metadata, nil
}

func filterWhereByTable(ctx context.Context, where ast.ExpressionNode, table string, alis string) error {
	metadata, err := loadMetadataByTable(ctx, table)
	if err != nil {
		return errors.WithStack(err)
	}

	if err = filterNodeByTable(where, metadata, alis); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

var replaceNode = &ast.BinaryComparisonPredicateNode{
	Left:  &ast.AtomPredicateNode{A: &ast.ConstantExpressionAtom{Inner: 1}},
	Right: &ast.AtomPredicateNode{A: &ast.ConstantExpressionAtom{Inner: 1}},
	Op:    cmp.Ceq,
}

func filterNodeByTable(expNode ast.ExpressionNode, metadata *proto.TableMetadata, alis string) error {
	predicateNode, ok := expNode.(*ast.PredicateExpressionNode)
	if ok {
		bcpn, bcOk := predicateNode.P.(*ast.BinaryComparisonPredicateNode)
		if bcOk {
			columnNode, ok := bcpn.Left.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			if !ok {
				return errors.New("invalid node")
			}
			if columnNode.Prefix() != "" {
				if columnNode.Prefix() != metadata.Name && columnNode.Prefix() != alis {
					predicateNode.P = replaceNode
				}
			} else {
				_, ok := metadata.Columns[columnNode.Suffix()]
				if !ok {
					predicateNode.P = replaceNode
				}
			}
			rightColumn, ok := bcpn.Right.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			if ok {
				if rightColumn.Prefix() != "" {
					if rightColumn.Prefix() != metadata.Name && rightColumn.Prefix() != alis {
						return errors.New("not support node")
					}
				} else {
					_, ok := metadata.Columns[rightColumn.Suffix()]
					if !ok {
						return errors.New("not support node")
					}
				}
			}
			return nil
		}

		lpn, likeOk := predicateNode.P.(*ast.LikePredicateNode)
		if likeOk {
			columnNode := lpn.Left.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			if columnNode.Prefix() != "" {
				if columnNode.Prefix() != metadata.Name && columnNode.Prefix() != alis {
					predicateNode.P = replaceNode
				}
			} else {
				_, ok := metadata.Columns[columnNode.Suffix()]
				if !ok {
					predicateNode.P = replaceNode
				}
			}
			return nil
		}

		ipn, inOk := predicateNode.P.(*ast.InPredicateNode)
		if inOk {
			columnNode, ok := ipn.P.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			if !ok {
				return errors.New("invalid node")
			}
			if columnNode.Prefix() != "" {
				if columnNode.Prefix() != metadata.Name && columnNode.Prefix() != alis {
					predicateNode.P = replaceNode
				}
			} else {
				_, ok := metadata.Columns[columnNode.Suffix()]
				if !ok {
					predicateNode.P = replaceNode
				}
			}
			return nil
		}

		bpn, betweenOk := predicateNode.P.(*ast.BetweenPredicateNode)
		if betweenOk {
			columnNode, ok := bpn.Key.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			if !ok {
				return errors.New("invalid node")
			}
			if columnNode.Prefix() != "" {
				if columnNode.Prefix() != metadata.Name && columnNode.Prefix() != alis {
					predicateNode.P = replaceNode
				}
			} else {
				_, ok := metadata.Columns[columnNode.Suffix()]
				if !ok {
					predicateNode.P = replaceNode
				}
			}

			//columnNode := bpn.Right.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			return nil
		}

		rpn, regexpOk := predicateNode.P.(*ast.RegexpPredicationNode)
		if regexpOk {
			columnNode, ok := rpn.Left.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)
			if !ok {
				return errors.New("invalid node")
			}
			if columnNode.Prefix() != "" {
				if columnNode.Prefix() != metadata.Name && columnNode.Prefix() != alis {
					predicateNode.P = replaceNode
				}
			} else {
				_, ok := metadata.Columns[columnNode.Suffix()]
				if !ok {
					predicateNode.P = replaceNode
				}
			}
			return nil
		}

		return errors.New("invalid node")
	}

	node, ok := expNode.(*ast.LogicalExpressionNode)
	if !ok {
		return errors.New("invalid node")
	}

	if err := filterNodeByTable(node.Left, metadata, alis); err != nil {
		return err
	}
	if err := filterNodeByTable(node.Right, metadata, alis); err != nil {
		return err
	}
	return nil
}
