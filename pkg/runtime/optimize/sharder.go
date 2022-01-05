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
	"fmt"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/proto/rule"
	"github.com/dubbogo/arana/pkg/runtime/cmp"
	"github.com/dubbogo/arana/pkg/runtime/function"
	"github.com/dubbogo/arana/pkg/runtime/logical"
	"github.com/dubbogo/arana/pkg/runtime/misc"
	rrule "github.com/dubbogo/arana/pkg/runtime/rule"
	"github.com/dubbogo/arana/pkg/runtime/xxast"
)

// Sharder computes the shards from a SQL statement.
type Sharder rule.Rule

// Shard returns shards.
func (sh *Sharder) Shard(tableName xxast.TableName, filter xxast.ExpressionNode, args ...interface{}) (shards rule.DatabaseTables, fullScan bool, err error) {
	if filter == nil {
		return
	}

	var (
		sc shardCtx
		lo logical.Logical
		ev rrule.Evaluator
	)

	// 0. prepare shard context
	sc.tableName = tableName
	sc.args = args

	// 1. expression to logical
	if lo, err = sh.processExpression(&sc, filter); err != nil {
		err = errors.Wrap(err, "compute shard logical failed")
		return
	}
	// 2. logical to evaluator
	if ev, err = rrule.Eval(lo, tableName.Suffix(), sh.rule()); err != nil {
		err = errors.Wrap(err, "compute shard evaluator failed")
		return
	}
	// 3. eval
	if shards, err = ev.Eval(tableName.Suffix(), sh.rule()); err != nil && !errors.Is(err, rrule.ErrNoRuleMetadata) {
		err = errors.Wrap(err, "eval shards failed")
		return
	}

	// 4. return if not full-scan
	if !shards.IsFullScan() {
		return
	}

	// 5. check full-scan
	var shardKeysScaned bool
	for _, it := range sc.keys {
		if sh.rule().HasColumn(tableName.Suffix(), it) {
			shardKeysScaned = true
			break
		}
	}
	fullScan = !shardKeysScaned

	return
}

func (sh *Sharder) processExpression(sc *shardCtx, filter xxast.ExpressionNode) (logical.Logical, error) {
	switch n := filter.(type) {
	case *xxast.LogicalExpressionNode:
		return sh.processLogicalExpression(sc, n)
	case *xxast.PredicateExpressionNode:
		return sh.processPredicateExpression(sc, n)
	case *xxast.NotExpressionNode:
		return sh.processNotExpression(sc, n)
	default:
		return nil, errors.Errorf("processing expression %T is not supported yet", n)
	}
}

func (sh *Sharder) processNotExpression(sc *shardCtx, n *xxast.NotExpressionNode) (logical.Logical, error) {
	l, err := sh.processExpression(sc, n.E)
	if err != nil {
		return nil, err
	}
	return l.Not(), nil
}

func (sh *Sharder) processLogicalExpression(sc *shardCtx, n *xxast.LogicalExpressionNode) (logical.Logical, error) {
	left, err := sh.processExpression(sc, n.Left)
	if err != nil {
		return nil, err
	}
	right, err := sh.processExpression(sc, n.Right)
	if err != nil {
		return nil, err
	}

	switch n.Op {
	case logical.Land:
		return left.And(right), nil
	case logical.Lor:
		return left.Or(right), nil
	default:
		panic("unreachable")
	}
}

func (sh *Sharder) processPredicateExpression(sc *shardCtx, n *xxast.PredicateExpressionNode) (logical.Logical, error) {
	switch v := n.P.(type) {
	case *xxast.BinaryComparisonPredicateNode:
		return sh.processCompare(sc, v)
	case *xxast.InPredicateNode:
		return sh.processInPredicate(sc, v)
	case *xxast.AtomPredicateNode:
		return sh.processAtomPredicate(sc, v)
	case *xxast.BetweenPredicateNode:
		return sh.processBetweenPredicate(sc, v)
	case *xxast.LikePredicateNode:
		return sh.processLikePredicate(sc, v)
	default:
		panic(fmt.Sprintf("todo: unsupported predicate node %t", v))
	}
}

func (sh *Sharder) processExpressionAtom(sc *shardCtx, n xxast.ExpressionAtom) (logical.Logical, error) {
	switch a := n.(type) {
	case *xxast.NestedExpressionAtom:
		return sh.processExpression(sc, a.First)
	case *xxast.UnaryExpressionAtom:
		val, err := sh.getValueFromAtom(sc, a)
		if err != nil {
			var lo logical.Logical
			if lo, err = sh.processExpressionAtom(sc, a.Inner); err != nil {
				return nil, err
			}

			if a.IsOperatorNot() {
				return lo.Not(), nil
			}
			return lo, nil
		}
		if misc.IsZero(val) {
			return rrule.AlwaysFalseLogical, nil
		}
		return rrule.AlwaysTrueLogical, nil
	case *xxast.ConstantExpressionAtom:
		if misc.IsZero(a.Value()) {
			return rrule.AlwaysFalseLogical, nil
		}
		return rrule.AlwaysTrueLogical, nil
	case *xxast.MathExpressionAtom:
		val, err := function.Eval(a, sc.args...)
		if err != nil {
			return nil, err
		}
		if misc.IsZero(val) {
			return rrule.AlwaysFalseLogical, nil
		}
		return rrule.AlwaysTrueLogical, nil
	default:
		return nil, errors.Errorf("processing expression atom %T is not supported yet", a)
	}
}

func (sh *Sharder) processAtomPredicate(sc *shardCtx, n *xxast.AtomPredicateNode) (logical.Logical, error) {
	return sh.processExpressionAtom(sc, n.A)
}

func (sh *Sharder) processLikePredicate(sc *shardCtx, n *xxast.LikePredicateNode) (logical.Logical, error) {
	// pre-process LIKE, convert "xx LIKE 'abc'" to "xx = 'abc'"
	switch left := n.Left.(type) {
	case *xxast.AtomPredicateNode:
		switch key := left.A.(type) {
		case xxast.ColumnNameExpressionAtom:
			sc.appendKey(key.Suffix())
			if right, err := sh.getValue(sc, n.Right); err == nil {
				if like, ok := right.(string); ok {
					if !strings.Contains(like, "%") && !strings.Contains(like, "_") {
						return rrule.NewKeyed(key.Suffix(), cmp.Ceq, like).ToLogical(), nil
					}
				}
			}
		}
	}
	return rrule.AlwaysTrueLogical, nil
}

func (sh *Sharder) processBetweenPredicate(sc *shardCtx, n *xxast.BetweenPredicateNode) (logical.Logical, error) {
	switch key := n.Key.(type) {
	case *xxast.AtomPredicateNode:
		switch ka := key.A.(type) {
		case xxast.ColumnNameExpressionAtom:
			lv, err := sh.getValue(sc, n.Left)
			if err != nil {
				return nil, err
			}

			lr, err := sh.getValue(sc, n.Right)
			if err != nil {
				return nil, err
			}

			// write shard key
			sc.appendKey(ka.Suffix())

			if n.Not {
				// convert: f NOT BETWEEN a AND b -> f < a OR f > b
				k1 := rrule.NewKeyed(ka.Suffix(), cmp.Clt, lv)
				k2 := rrule.NewKeyed(ka.Suffix(), cmp.Cgt, lr)
				return k1.ToLogical().Or(k2.ToLogical()), nil
			}

			// convert: f BETWEEN a AND b -> f >= a AND f <= b
			k1 := rrule.NewKeyed(ka.Suffix(), cmp.Cgte, lv)
			k2 := rrule.NewKeyed(ka.Suffix(), cmp.Clte, lr)
			return k1.ToLogical().And(k2.ToLogical()), nil
		}
	}

	return nil, nil
}

func (sh *Sharder) getValueFromAtom(sc *shardCtx, atom xxast.ExpressionAtom) (interface{}, error) {
	switch it := atom.(type) {
	case *xxast.UnaryExpressionAtom:
		v, err := sh.getValueFromAtom(sc, it.Inner)
		if err != nil {
			return nil, err
		}
		return misc.ComputeUnary(it.Operator, v)
	case *xxast.ConstantExpressionAtom:
		return it.Value(), nil
	case xxast.VariableExpressionAtom:
		return sc.arg(it.N())
	case *xxast.MathExpressionAtom:
		return function.Eval(it, sc.args...)
	case *xxast.FunctionCallExpressionAtom:
		switch fn := it.F.(type) {
		case *xxast.Function:
			return function.EvalFunction(fn, sc.args...)
		case *xxast.AggrFunction:
			return nil, errors.New("aggregate function should not appear here")
		case *xxast.CastFunction:
			return function.EvalCastFunction(fn, sc.args...)
		case *xxast.CaseWhenElseFunction:
			return function.EvalCaseWhenFunction(fn, sc.args...)
		default:
			return nil, errors.Errorf("get value from %T is not supported yet", it)
		}
	case xxast.ColumnNameExpressionAtom:
		return nil, function.ErrCannotEvalWithColumnName
	case *xxast.NestedExpressionAtom:
		nested, ok := it.First.(*xxast.PredicateExpressionNode)
		if !ok {
			return nil, errors.Errorf("only those nest expressions within predicated expression node is supported")
		}
		return sh.getValue(sc, nested.P)
	default:
		return nil, errors.Errorf("extracting value from %T is not supported yet", it)
	}
}

func (sh *Sharder) getValue(sc *shardCtx, next xxast.PredicateNode) (interface{}, error) {
	switch v := next.(type) {
	case *xxast.AtomPredicateNode:
		return sh.getValueFromAtom(sc, v.A)
	}
	return nil, errors.Errorf("get value from %T is not supported yet", next)
}

func (sh *Sharder) processInPredicate(sc *shardCtx, n *xxast.InPredicateNode) (logical.Logical, error) {
	switch left := n.P.(type) {
	case *xxast.AtomPredicateNode:
		switch key := left.A.(type) {
		case xxast.ColumnNameExpressionAtom:
			var ret logical.Logical
			for _, exp := range n.E {
				switch next := exp.(type) {
				case *xxast.PredicateExpressionNode:
					actualValue, err := sh.getValue(sc, next.P)
					if err != nil {
						return nil, err
					}
					if n.IsNot() {
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
				default:
					panic(fmt.Sprintf("todo: unsupported expression node %t!", next))
				}
			}

			// write key
			sc.appendKey(key.Suffix())

			return ret, nil
		}
	}
	return nil, nil
}

func (sh *Sharder) processCompare(sc *shardCtx, n *xxast.BinaryComparisonPredicateNode) (logical.Logical, error) {
	left := n.Left.(*xxast.AtomPredicateNode)
	switch la := left.A.(type) {
	case xxast.ColumnNameExpressionAtom:
		val, err := sh.getValue(sc, n.Right)
		if function.IsEvalWithColumnErr(err) {
			return rrule.AlwaysTrueLogical, nil
		} else if err != nil {
			return nil, err
		}

		// write key
		sc.appendKey(la.Suffix())

		return rrule.NewKeyed(la.Suffix(), n.Op, val).ToLogical(), nil
	default:
		leftValue, err := sh.getValue(sc, n.Left)
		if function.IsEvalWithColumnErr(err) {
			return rrule.AlwaysTrueLogical, nil
		} else if err != nil {
			return nil, err
		}
		rightValue, err := sh.getValue(sc, n.Right)
		if function.IsEvalWithColumnErr(err) {
			return rrule.AlwaysTrueLogical, nil
		} else if err != nil {
			return nil, err
		}

		var bingo bool

		compareResult := misc.Compare(leftValue, rightValue)
		switch n.Op {
		case cmp.Ceq:
			bingo = compareResult == 0
		case cmp.Cne:
			bingo = compareResult != 0
		case cmp.Cgt:
			bingo = compareResult > 0
		case cmp.Cgte:
			bingo = compareResult >= 0
		case cmp.Clt:
			bingo = compareResult < 0
		case cmp.Clte:
			bingo = compareResult <= 0
		default:
			panic(fmt.Sprintf("unsupported binary compare operation %s!", n.Op))
		}

		if bingo {
			return rrule.AlwaysTrueLogical, nil
		}
		return rrule.AlwaysFalseLogical, nil
	}
}

func (sh *Sharder) rule() *rule.Rule {
	return (*rule.Rule)(sh)
}

type shardCtx struct {
	tableName xxast.TableName
	args      []interface{}
	keys      []string
}

func (sc *shardCtx) arg(i int) (interface{}, error) {
	if i < 0 || i >= len(sc.args) {
		return nil, errors.New("arg i is out of range")
	}
	return sc.args[i], nil
}

func (sc *shardCtx) appendKey(key string) {
	sc.keys = append(sc.keys, key)
}
