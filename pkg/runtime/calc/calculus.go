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

package calc

import (
	stdErrors "errors"
	"strings"
)

import (
	"github.com/pkg/errors"

	"golang.org/x/sync/errgroup"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/calc/logic"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/util/misc"
)

var ErrNoShardMatched = stdErrors.New("no virtual shard matched")

var Zero = &Calculus{
	s: rule.NewShards(),
}

func Wrap(c *cmp.Comparative) logic.Logic[*Calculus] {
	return logic.Wrap[*Calculus](&Calculus{
		c: c,
	})
}

type Calculus struct {
	c *cmp.Comparative
	s *rule.Shards
}

func (ca *Calculus) Compare(item logic.Item) int {
	return compareComparative(ca.c, item.(*Calculus).c)
}

func (ca *Calculus) String() string {
	var sb strings.Builder
	sb.WriteString("Calculus{")
	if ca.c != nil {
		sb.WriteString("c=")
		sb.WriteString(ca.c.String())
	}
	if ca.s != nil {
		if ca.c != nil {
			sb.WriteByte(',')
		}
		sb.WriteString("s=")
		sb.WriteString(ca.s.String())
	}
	sb.WriteByte('}')
	return sb.String()
}

type calculusOperator rule.VTable

func (co *calculusOperator) AND(first *Calculus, others ...*Calculus) (*Calculus, error) {
	groups := make(map[string]map[cmp.Comparison][]*Calculus)
	add := func(c *Calculus) {
		if c == nil {
			return
		}

		key := c.c.Key()
		comparison := c.c.Comparison()
		if _, ok := groups[key]; !ok {
			groups[key] = map[cmp.Comparison][]*Calculus{
				comparison: {c},
			}
			return
		}

		if _, ok := groups[key][comparison]; !ok {
			groups[key][comparison] = append(groups[key][comparison], c)
			return
		}

		switch comparison {
		case cmp.Ceq:
		case cmp.Cne:
			groups[key][comparison] = append(groups[key][comparison], c)
		case cmp.Cgt, cmp.Cgte: // minimum is the first value.
		case cmp.Clt, cmp.Clte: // maximum is the last value.
			groups[key][comparison][0] = c
		}
	}
	add(first)
	for i := range others {
		add(others[i])
	}

	vShard := searchVShard((*rule.VTable)(co), groups)

	if vShard == nil {
		return nil, ErrNoShardMatched
	}

	type valuePair struct {
		db, tbl []interface{}
	}

	values := make(map[string]valuePair)
	for i := range vShard.Variables() {
		name := vShard.Variables()[i]
		cm := calculusMap(groups[name])
		begin, end := cm.getRange()
		switch {
		case begin != nil && end != nil:
			var vp valuePair
			if vShard.DB != nil {
				vp.db = computeRange(vShard.DB, begin.c, end.c)
			}
			if vShard.Table != nil {
				vp.tbl = computeRange(vShard.Table, begin.c, end.c)
			}
			values[name] = vp
		case begin != nil && end == nil:
			var vp valuePair
			if vShard.DB != nil {
				vp.db = computeLRange(vShard.DB, begin.c)
			}
			if vShard.Table != nil {
				vp.tbl = computeLRange(vShard.Table, begin.c)
			}
			values[name] = vp
		case begin == nil && end != nil:
			var vp valuePair
			if vShard.DB != nil {
				vp.db = computeRRange(vShard.DB, end.c)
			}
			if vShard.Table != nil {
				vp.tbl = computeRRange(vShard.Table, end.c)
			}
			values[name] = vp
		case begin == nil && end == nil:
			if cm.has(cmp.Ceq) {
				return Zero, nil
			}
			return nil, nil
		}
	}

	compute := func(computer rule.ShardComputer, dst *[]int, vals [][]interface{}) error {
		var args []proto.Value
		for _, next := range misc.CartesianProduct(vals) {
			for i := range next {
				arg, err := proto.NewValue(next[i])
				if err != nil {
					return err
				}
				args = append(args, arg)
			}
			idx, err := computer.Compute(args...)
			if err != nil {
				return err
			}
			*dst = append(*dst, idx)
			args = args[:0]
		}
		return nil
	}

	computeDB := func(computer rule.ShardComputer, dst *[]int) error {
		var vals [][]interface{}
		for _, name := range computer.Variables() {
			vals = append(vals, values[name].db)
		}
		return compute(computer, dst, vals)
	}

	computeTable := func(computer rule.ShardComputer, dst *[]int) error {
		var vals [][]interface{}
		for _, name := range computer.Variables() {
			vals = append(vals, values[name].tbl)
		}
		return compute(computer, dst, vals)
	}

	var (
		g                     errgroup.Group
		dbIndexes, tblIndexes []int
	)

	g.Go(func() error {
		if vShard.DB == nil {
			dbIndexes = append(dbIndexes, 0)
			return nil
		}
		return computeDB(vShard.DB.Computer, &dbIndexes)
	})
	g.Go(func() error {
		if vShard.Table == nil {
			tblIndexes = append(tblIndexes, 0)
			return nil
		}
		return computeTable(vShard.Table.Computer, &tblIndexes)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	shards := rule.NewShards()
	cp := misc.CartesianProduct[int]([][]int{dbIndexes, tblIndexes})
	topology := (*rule.VTable)(co).Topology()
	for i := range cp {
		x := cp[i][0]
		y := cp[i][1]
		if topology.Exists(x, y) {
			shards.Add(uint32(x), uint32(y))
		}
	}

	if shards.Len() < 1 {
		return Zero, nil
	}

	return &Calculus{
		s: shards,
	}, nil
}

func (co calculusOperator) OR(first *Calculus, others ...*Calculus) (*Calculus, error) {
	if first == nil {
		return nil, nil
	}

	shards := rule.NewShards()

	if first.c != nil {
		var err error
		if first, err = co.AND(first); err != nil {
			return nil, err
		}
	}

	if first.s != nil {
		first.s.Each(func(db, tb uint32) bool {
			shards.Add(db, tb)
			return true
		})
	}

	for _, next := range others {
		if next == nil {
			return nil, nil
		}
		if next.c != nil {
			var err error
			if next, err = co.AND(next); err != nil {
				return nil, err
			}
		}
		if next.s != nil {
			next.s.Each(func(db, tb uint32) bool {
				shards.Add(db, tb)
				return true
			})
		}
	}

	return &Calculus{
		s: shards,
	}, nil
}

func (co calculusOperator) NOT(input *Calculus) (*Calculus, error) {
	if input.c != nil {
		var nextCmp cmp.Comparison
		switch input.c.Comparison() {
		case cmp.Ceq:
			nextCmp = cmp.Cne
		case cmp.Cne:
			nextCmp = cmp.Ceq
		case cmp.Cgt:
			nextCmp = cmp.Clte
		case cmp.Cgte:
			nextCmp = cmp.Clt
		case cmp.Clt:
			nextCmp = cmp.Cgte
		case cmp.Clte:
			nextCmp = cmp.Cgt
		default:
			panic("unreachable")
		}

		c := new(cmp.Comparative)
		*c = *input.c
		c.SetComparison(nextCmp)

		return &Calculus{
			c: c,
		}, nil
	}
	_ = input
	panic("implement me")
}

type calculusMap map[cmp.Comparison][]*Calculus

func (cm calculusMap) has(c cmp.Comparison) bool {
	_, ok := cm[c]
	return ok
}

func (cm calculusMap) getRange() (begin, end *Calculus) {
	if eq, hasEq := cm[cmp.Ceq]; hasEq {
		if neList, hasNe := cm[cmp.Cne]; hasNe {
			for _, ne := range neList {
				if ne.c == nil {
					continue
				}
				// a == 1 && a <> 1
				if compareComparativeValue(ne.c, eq[0].c) == 0 {
					return
				}
			}
		}

		if gte, hasGte := cm[cmp.Cgte]; hasGte {
			switch compareComparativeValue(eq[0].c, gte[0].c) {
			// a==1 && a>=2 ---> NaN
			case -1:
				return
			}
		}
		if gt, hasGt := cm[cmp.Cgt]; hasGt {
			switch compareComparativeValue(eq[0].c, gt[0].c) {
			// a==2 && a>2 ---> NaN
			// a==1 && a>2 ---> NaN
			case 0, -1:
				return
			}
		}

		if lte, hasLte := cm[cmp.Clte]; hasLte {
			switch compareComparativeValue(eq[0].c, lte[0].c) {
			// a==2 && a<1 ---> NaN
			case 1:
				return
			}
		}

		if lt, hasLt := cm[cmp.Clt]; hasLt {
			switch compareComparativeValue(eq[0].c, lt[0].c) {
			// a==2 && a<2 ---> NaN
			// a==2 && a<1 ---> NaN
			case 0, 1:
				return
			}
		}

		begin = eq[0]

		return
	}

	gte, hasGte := cm[cmp.Cgte]
	gt, hasGt := cm[cmp.Cgt]
	switch {
	case hasGte && hasGt:
		switch compareComparativeValue(gte[0].c, gt[0].c) {
		// a >= 1 && a > 1 ---> a > 1
		// a >= 1 && a > 2 ---> a > 2
		case 0, -1:
			begin = gt[0]
		// a >= 2 && a > 1 ---> a >= 2
		case 1:
			begin = gte[0]
		}
	case hasGte:
		begin = gte[0]
	case hasGt:
		begin = gt[0]
	}

	lte, hasLte := cm[cmp.Clte]
	lt, hasLt := cm[cmp.Clt]

	switch {
	case hasLte && hasLt:
		switch compareComparativeValue(lte[0].c, lt[0].c) {
		// a<=1 && a<1 ---> a < 1
		// a<=2 && a<1 ---> a < 1
		case 0, 1:
			end = lt[0]
		// a<=1 && a<2 ---> a <= 1
		case -1:
			end = lte[0]
		}
	case hasLte:
		end = lte[0]
	case hasLt:
		end = lt[0]
	}

	return
}

func Eval(vtab *rule.VTable, l logic.Logic[*Calculus]) (*rule.Shards, error) {
	sh, err := innerEval(vtab, l)
	if err == nil {
		return sh, nil
	}
	if errors.Is(err, logic.ErrEmptyIntersection) {
		return rule.NewShards(), nil
	}
	if errors.Is(err, logic.ErrEmptyUnion) {
		return nil, nil
	}
	return nil, err
}

func innerEval(vtab *rule.VTable, l logic.Logic[*Calculus]) (*rule.Shards, error) {
	c, err := l.Eval((*calculusOperator)(vtab))
	if err != nil {
		return nil, err
	}

	if c == nil {
		return nil, nil
	}

	if c.s != nil {
		return c.s, nil
	}

	if c, err = (*calculusOperator)(vtab).AND(c); err != nil {
		return nil, err
	}

	if c == nil {
		return nil, nil
	}

	return c.s, nil
}
