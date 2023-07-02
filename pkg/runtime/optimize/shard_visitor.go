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
	"github.com/arana-db/arana/pkg/runtime/calc"
	"github.com/arana-db/arana/pkg/runtime/calc/logic"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/runtime/misc"
	"github.com/arana-db/arana/pkg/runtime/misc/extvalue"
)

var _ ast.Visitor = (*ShardVisitor)(nil)

type Calculus = logic.Logic[*calc.Calculus]

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

func (sd *ShardVisitor) Result() []misc.Pair[ast.TableName, *rule.Shards] {
	return sd.results
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

	// 2. eval shards
	shards, err := calc.Eval(vtab, l.(logic.Logic[*calc.Calculus]))
	if err != nil {
		return errors.Wrap(err, "compute shard evaluator failed")
	}
	// 3. eval
	if err != nil && !errors.Is(err, calc.ErrNoShardMatched) {
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

	ll, lr := left.(Calculus), right.(Calculus)
	if node.Or {
		return logic.OR(ll, lr), nil
	}
	return logic.AND(ll, lr), nil
}

func (sd *ShardVisitor) VisitNotExpression(node *ast.NotExpressionNode) (interface{}, error) {
	ret, err := node.E.Accept(sd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return logic.NOT(ret.(Calculus)), nil
}

func (sd *ShardVisitor) VisitPredicateExpression(node *ast.PredicateExpressionNode) (interface{}, error) {
	return node.P.Accept(sd)
}

func (sd *ShardVisitor) VisitSelectElementExpr(node *ast.SelectElementExpr) (interface{}, error) {
	switch inner := node.Expression().(type) {
	case *ast.PredicateExpressionNode:
		return sd.VisitPredicateExpression(inner)
	case *ast.LogicalExpressionNode:
		return sd.VisitLogicalExpression(inner)
	}
	return node.Expression().Accept(sd)
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
		k1, err := newCmp(key.Suffix(), cmp.Clt, l)
		if err != nil {
			return nil, err
		}
		k2, err := newCmp(key.Suffix(), cmp.Cgt, r)
		if err != nil {
			return nil, err
		}
		return logic.OR(
			calc.Wrap(k1),
			calc.Wrap(k2),
		), nil
	}

	// convert: f BETWEEN a AND b -> f >= a AND f <= b
	k1, err := newCmp(key.Suffix(), cmp.Cgte, l)
	if err != nil {
		return nil, err
	}
	k2, err := newCmp(key.Suffix(), cmp.Clte, r)
	if err != nil {
		return nil, err
	}
	return logic.AND(
		calc.Wrap(k1),
		calc.Wrap(k2),
	), nil
}

func newCmp(key string, comparison cmp.Comparison, v proto.Value) (*cmp.Comparative, error) {
	switch v.Family() {
	case proto.ValueFamilyString:
		return cmp.NewString(key, comparison, v.String()), nil
	case proto.ValueFamilySign, proto.ValueFamilyUnsigned, proto.ValueFamilyFloat, proto.ValueFamilyDecimal:
		i, err := v.Int64()
		if err != nil {
			return nil, err
		}
		return cmp.NewInt64(key, comparison, i), nil

	case proto.ValueFamilyBool:
		b, err := v.Bool()
		if err != nil {
			return nil, err
		}
		if b {
			return cmp.NewInt64(key, comparison, 1), nil
		}
		return cmp.NewInt64(key, comparison, 0), nil

	case proto.ValueFamilyTime:
		t, err := v.Time()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return cmp.NewDate(key, comparison, t), nil
	default:
		return cmp.NewString(key, comparison, v.String()), nil
	}
}

func (sd *ShardVisitor) VisitPredicateBinaryComparison(node *ast.BinaryComparisonPredicateNode) (interface{}, error) {
	switch k := node.Left.(*ast.AtomPredicateNode).A.(type) {
	case ast.ColumnNameExpressionAtom:
		v, err := extvalue.Compute(sd.ctx, node.Right, sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return alwaysTrue(), nil
			}
			return nil, errors.WithStack(err)
		}
		c, err := newCmp(k.Suffix(), node.Op, v)
		if err != nil {
			return nil, err
		}
		return calc.Wrap(c), nil
	}

	switch k := node.Right.(*ast.AtomPredicateNode).A.(type) {
	case ast.ColumnNameExpressionAtom:
		v, err := extvalue.Compute(sd.ctx, node.Left, sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return alwaysTrue(), nil
			}
			return nil, errors.WithStack(err)
		}
		c, err := newCmp(k.Suffix(), node.Op, v)
		if err != nil {
			return nil, err
		}
		return calc.Wrap(c), nil
	}

	l, _ := extvalue.Compute(sd.ctx, node.Left, sd.args...)
	r, _ := extvalue.Compute(sd.ctx, node.Right, sd.args...)
	if l == nil || r == nil {
		return alwaysTrue(), nil
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
		return alwaysTrue(), nil
	}
	return alwaysFalse(), nil
}

func (sd *ShardVisitor) VisitPredicateIn(node *ast.InPredicateNode) (interface{}, error) {
	key := node.P.(*ast.AtomPredicateNode).A.(ast.ColumnNameExpressionAtom)

	var ret Calculus
	for i := range node.E {
		actualValue, err := extvalue.Compute(sd.ctx, node.E[i], sd.args...)
		if err != nil {
			if extvalue.IsErrNotSupportedValue(err) {
				return alwaysTrue(), nil
			}
			return nil, errors.WithStack(err)
		}

		if node.Not {
			ke, err := newCmp(key.Suffix(), cmp.Cne, actualValue)
			if err != nil {
				return nil, err
			}
			// convert: f NOT IN (a,b,c) -> f <> a AND f <> b AND f <> c
			if ret == nil {
				ret = calc.Wrap(ke)
			} else {
				ret = logic.AND(ret, calc.Wrap(ke))
			}
		} else {
			// convert: f IN (a,b,c) -> f = a OR f = b OR f = c
			ke, err := newCmp(key.Suffix(), cmp.Ceq, actualValue)
			if err != nil {
				return nil, err
			}
			if ret == nil {
				ret = calc.Wrap(ke)
			} else {
				ret = logic.OR(ret, calc.Wrap(ke))
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
			return alwaysTrue(), nil
		}
		return nil, errors.WithStack(err)
	}

	if like == nil {
		return alwaysTrue(), nil
	}

	if !strings.ContainsAny(like.String(), "%_") {
		c, err := newCmp(key.Suffix(), cmp.Ceq, like)
		if err != nil {
			return nil, err
		}

		return calc.Wrap(c), nil
	}

	return alwaysTrue(), nil
}

func (sd *ShardVisitor) VisitPredicateRegexp(_ *ast.RegexpPredicationNode) (interface{}, error) {
	return alwaysTrue(), nil
}

func (sd *ShardVisitor) VisitAtomColumn(_ ast.ColumnNameExpressionAtom) (interface{}, error) {
	return alwaysTrue(), nil
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
			return alwaysTrue(), nil
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

func (sd *ShardVisitor) VisitAtomSystemVariable(_ *ast.SystemVariableExpressionAtom) (interface{}, error) {
	return alwaysTrue(), nil
}

func (sd *ShardVisitor) VisitAtomVariable(node ast.VariableExpressionAtom) (interface{}, error) {
	return sd.fromConstant(sd.args[node.N()])
}

func (sd *ShardVisitor) VisitAtomInterval(_ *ast.IntervalExpressionAtom) (interface{}, error) {
	return alwaysTrue(), nil
}

func (sd *ShardVisitor) fromConstant(val proto.Value) (Calculus, error) {
	if val == nil {
		return alwaysFalse(), nil
	}

	if b, err := val.Bool(); err == nil && !b {
		return alwaysFalse(), nil
	}
	return alwaysTrue(), nil
}

func (sd *ShardVisitor) fromValueNode(node ast.Node) (interface{}, error) {
	val, err := extvalue.Compute(sd.ctx, node, sd.args...)
	if err != nil {
		if extvalue.IsErrNotSupportedValue(err) {
			return alwaysTrue(), nil
		}
		return nil, errors.WithStack(err)
	}
	return sd.fromConstant(val)
}

func alwaysTrue() Calculus {
	return logic.True[*calc.Calculus]()
}

func alwaysFalse() Calculus {
	return logic.False[*calc.Calculus]()
}
