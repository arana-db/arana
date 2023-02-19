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
	"strings"
)

import (
	"github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/runtime/logical"
	"github.com/arana-db/arana/pkg/runtime/misc"
	"github.com/arana-db/arana/pkg/runtime/misc/extvalue"
	rrule "github.com/arana-db/arana/pkg/runtime/rule"
)

var _ ast.Visitor = (*ShardVisitor)(nil)

type ShardVisitor struct {
	ctx context.Context
	ast.BaseVisitor
	ru      *rule.Rule
	args    []proto.Value
	results []misc.Pair[ast.TableName, *rule.Shards]
}

func NewXSharder(ctx context.Context, ru *rule.Rule, args []proto.Value) *ShardVisitor {
	return &ShardVisitor{
		ctx:  ctx,
		ru:   ru,
		args: args,
	}
}

func (sd *ShardVisitor) SimpleShard(table ast.TableName, where ast.ExpressionNode) (rule.DatabaseTables, error) {
	var (
		shards rule.DatabaseTables
		err    error
	)
	if err = sd.ForSingleSelect(table, "", where); err != nil {
		return nil, errors.Wrapf(err, "cannot calculate shards of table '%s'", table.Suffix())
	}
	vt, _ := sd.ru.VTable(table.Suffix())
	for i := range sd.results {
		if sd.results[i].L.Suffix() == table.Suffix() {
			if r := sd.results[i].R; r != nil {
				shards = make(rule.DatabaseTables)
				r.Each(func(db, tb uint32) bool {
					dbs, tbs, ok := vt.Topology().Render(int(db), int(tb))
					if !ok {
						err = errors.Errorf("cannot render table '%s'", vt.Name())
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

	return shards, nil
}

func (sd *ShardVisitor) ForSingleSelect(table ast.TableName, alias string, where ast.ExpressionNode) error {
	vtab, ok := sd.ru.VTable(table.Suffix())
	if !ok {
		shards := rule.NewShards()
		shards.Add(0, 0)
		sd.results = append(sd.results, misc.Pair[ast.TableName, *rule.Shards]{
			L: table,
			R: shards,
		})
		return nil
	}

	if where == nil {
		sd.results = append(sd.results, misc.Pair[ast.TableName, *rule.Shards]{
			L: table,
		})
		return nil
	}

	l, err := where.Accept(sd)
	if err != nil {
		return errors.WithStack(err)
	}

	ev, err := rrule.Eval(l.(logical.Logical), vtab)
	// 2. logical to evaluator
	if err != nil {
		return errors.Wrap(err, "compute shard evaluator failed")
	}
	// 3. eval
	shards, err := ev.Eval(vtab)
	if err != nil && !errors.Is(err, rrule.ErrNoRuleMetadata) {
		return errors.Wrap(err, "eval shards failed")
	}

	sd.results = append(sd.results, misc.Pair[ast.TableName, *rule.Shards]{
		L: table,
		R: shards,
	})

	return nil
}

func (sd *ShardVisitor) VisitSelectStatement(node *ast.SelectStatement) (interface{}, error) {
	switch len(node.From) {
	case 0:
		return nil, nil
	case 1:
		return nil, sd.ForSingleSelect(node.From[0].Source.(ast.TableName), node.From[0].Alias, node.Where)
	default:
		// TODO: need implementation multiple select from
		panic("implement me: multiple select from")
	}
}

func (sd *ShardVisitor) VisitLogicalExpression(node *ast.LogicalExpressionNode) (interface{}, error) {
	left, err := node.Left.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	right, err := node.Right.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ll, lr := left.(logical.Logical), right.(logical.Logical)
	switch node.Op {
	case logical.Lor:
		return ll.Or(lr), nil
	case logical.Land:
		return ll.And(lr), nil
	default:
		panic("unreachable")
	}
}

func (sd *ShardVisitor) VisitNotExpression(node *ast.NotExpressionNode) (interface{}, error) {
	ret, err := node.E.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return ret.(logical.Logical).Not(), nil
}

func (sd *ShardVisitor) VisitPredicateExpression(node *ast.PredicateExpressionNode) (interface{}, error) {
	return node.P.Accept(sd)
}

func (sd *ShardVisitor) VisitPredicateAtom(node *ast.AtomPredicateNode) (interface{}, error) {
	return node.A.Accept(sd)
}

func (sd *ShardVisitor) VisitPredicateBetween(node *ast.BetweenPredicateNode) (interface{}, error) {
	key := node.Key.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	l, err := extvalue.Compute(sd.ctx, node.Left, sd.args...)
	if err != nil {
		return nil, err
	}

	r, err := extvalue.Compute(sd.ctx, node.Right, sd.args...)
	if err != nil {
		return nil, err
	}

	if node.Not {
		// convert: f NOT BETWEEN a AND b -> f < a OR f > b
		k1 := rrule.NewKeyed(key.Suffix(), cmp.Clt, l)
		k2 := rrule.NewKeyed(key.Suffix(), cmp.Cgt, r)
		return k1.ToLogical().Or(k2.ToLogical()), nil
	}

	// convert: f BETWEEN a AND b -> f >= a AND f <= b
	k1 := rrule.NewKeyed(key.Suffix(), cmp.Cgte, l)
	k2 := rrule.NewKeyed(key.Suffix(), cmp.Clte, r)
	return k1.ToLogical().And(k2.ToLogical()), nil
}

func (sd *ShardVisitor) VisitPredicateBinaryComparison(node *ast.BinaryComparisonPredicateNode) (interface{}, error) {
	switch k := node.Left.(*ast.AtomPredicateNode).A.(type) {
	case ast.ColumnNameExpressionAtom:
		v, err := extvalue.Compute(sd.ctx, node.Right, sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return rrule.AlwaysTrueLogical, nil
			}
			return nil, errors.WithStack(err)
		}
		return rrule.NewKeyed(k.Suffix(), node.Op, v).ToLogical(), nil
	}

	switch k := node.Right.(*ast.AtomPredicateNode).A.(type) {
	case ast.ColumnNameExpressionAtom:
		v, err := extvalue.Compute(sd.ctx, node.Left, sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return rrule.AlwaysTrueLogical, nil
			}
			return nil, errors.WithStack(err)
		}
		return rrule.NewKeyed(k.Suffix(), node.Op, v).ToLogical(), nil
	}

	l, _ := extvalue.Compute(sd.ctx, node.Left, sd.args...)
	r, _ := extvalue.Compute(sd.ctx, node.Right, sd.args...)
	if l == nil || r == nil {
		return rrule.AlwaysTrueLogical, nil
	}

	var (
		isStr bool
		c     int
	)

	switch l.Family() {
	case proto.ValueFamilyString:
		switch r.Family() {
		case proto.ValueFamilyString:
			isStr = true
		}
	}

	if isStr {
		c = strings.Compare(l.String(), r.String())
	} else {
		x, err := l.Decimal()
		if err == nil {
			x = decimal.Zero
		}
		y, err := r.Decimal()
		if err == nil {
			y = decimal.Zero
		}
		switch {
		case x.GreaterThan(y):
			c = 1
		case x.LessThan(y):
			c = -1
		}
	}
	var bingo bool
	switch node.Op {
	case cmp.Ceq:
		bingo = c == 0
	case cmp.Cne:
		bingo = c != 0
	case cmp.Cgt:
		bingo = c > 0
	case cmp.Cgte:
		bingo = c >= 0
	case cmp.Clt:
		bingo = c < 0
	case cmp.Clte:
		bingo = c <= 0
	}

	if bingo {
		return rrule.AlwaysTrueLogical, nil
	}
	return rrule.AlwaysFalseLogical, nil
}

func (sd *ShardVisitor) VisitPredicateIn(node *ast.InPredicateNode) (interface{}, error) {
	key := node.P.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	var ret logical.Logical
	for i := range node.E {
		actualValue, err := extvalue.Compute(sd.ctx, node.E[i], sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return rrule.AlwaysTrueLogical, nil
			}
			return nil, errors.WithStack(err)
		}
		if node.Not {
			// convert: f NOT IN (a,b,c) -> f <> a AND f <> b AND f <> c
			ke := rrule.NewKeyed(key.Suffix(), cmp.Cne, actualValue)
			if ret == nil {
				ret = ke.ToLogical()
			} else {
				ret = ret.And(ke.ToLogical())
			}
		} else {
			// convert: f IN (a,b,c) -> f = a OR f = b OR f = c
			ke := rrule.NewKeyed(key.Suffix(), cmp.Ceq, actualValue)
			if ret == nil {
				ret = ke.ToLogical()
			} else {
				ret = ret.Or(ke.ToLogical())
			}
		}
	}

	return ret, nil
}

func (sd *ShardVisitor) VisitPredicateLike(node *ast.LikePredicateNode) (interface{}, error) {
	key := node.Left.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	like, err := extvalue.Compute(sd.ctx, node.Right, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return rrule.AlwaysTrueLogical, nil
		}
		return nil, errors.WithStack(err)
	}

	if like == nil {
		return rrule.AlwaysTrueLogical, nil
	}

	if !strings.ContainsAny(like.String(), "%_") {
		return rrule.NewKeyed(key.Suffix(), cmp.Ceq, like).ToLogical(), nil
	}

	return rrule.AlwaysTrueLogical, nil
}

func (sd *ShardVisitor) VisitPredicateRegexp(node *ast.RegexpPredicationNode) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *ShardVisitor) VisitAtomColumn(node ast.ColumnNameExpressionAtom) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *ShardVisitor) VisitAtomConstant(node *ast.ConstantExpressionAtom) (interface{}, error) {
	v, err := proto.NewValue(node.Value())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	l, err := sd.fromConstant(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return l, nil
}

func (sd *ShardVisitor) VisitAtomFunction(node *ast.FunctionCallExpressionAtom) (interface{}, error) {
	val, err := extvalue.Compute(sd.ctx, node, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return rrule.AlwaysTrueLogical, nil
		}
		return nil, errors.WithStack(err)
	}
	return sd.fromConstant(val)
}

func (sd *ShardVisitor) VisitAtomNested(node *ast.NestedExpressionAtom) (interface{}, error) {
	return node.First.Accept(sd)
}

func (sd *ShardVisitor) VisitAtomUnary(node *ast.UnaryExpressionAtom) (interface{}, error) {
	return sd.fromValueNode(node)
}

func (sd *ShardVisitor) VisitAtomMath(node *ast.MathExpressionAtom) (interface{}, error) {
	return sd.fromValueNode(node)
}

func (sd *ShardVisitor) VisitAtomSystemVariable(node *ast.SystemVariableExpressionAtom) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *ShardVisitor) VisitAtomVariable(node ast.VariableExpressionAtom) (interface{}, error) {
	return sd.fromConstant(sd.args[node.N()])
}

func (sd *ShardVisitor) VisitAtomInterval(node *ast.IntervalExpressionAtom) (interface{}, error) {
	return rrule.AlwaysTrueLogical, nil
}

func (sd *ShardVisitor) fromConstant(val proto.Value) (logical.Logical, error) {
	if val == nil {
		return rrule.AlwaysFalseLogical, nil
	}

	if b, err := val.Bool(); err == nil && !b {
		return rrule.AlwaysFalseLogical, nil
	}

	return rrule.AlwaysTrueLogical, nil
}

func (sd *ShardVisitor) fromValueNode(node ast.Node) (interface{}, error) {
	val, err := extvalue.Compute(sd.ctx, node, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return rrule.AlwaysTrueLogical, nil
		}
		return nil, errors.WithStack(err)
	}
	return sd.fromConstant(val)
}
