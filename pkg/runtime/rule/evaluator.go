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

package rule

import (
	stdErrors "errors"
	"fmt"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/runtime/logical"
	"github.com/arana-db/arana/pkg/runtime/misc"
)

var (
	_emptyEvaluator Evaluator = emptyEvaluator{}
	_noopEvaluator  Evaluator = noopEvaluator{}

	AlwaysTrueLogical  = logical.New("1", logical.WithValue(_noopEvaluator))
	AlwaysFalseLogical = logical.New("0", logical.WithValue(_emptyEvaluator))
)

var (
	_ Evaluator = (*KeyedEvaluator)(nil)
	_ Evaluator = noopEvaluator{}
	_ Evaluator = (*staticEvaluator)(nil)
)

var ErrNoRuleMetadata = stdErrors.New("no rule metadata found")

var emptyDatabaseTables rule.DatabaseTables

func init() {
	emptyDatabaseTables = make(map[string][]string)
}

func toRangeIterator(begin, end rule.Range) rule.Range {
	var a []interface{}

	for begin.HasNext() {
		a = append(a, begin.Next())
	}

	if len(a) < 1 {
		return Multiple()
	}

	var max interface{}
	if !end.HasNext() {
		return Multiple()
	}

	max = end.Next()

	var merged []interface{}

	for i := 0; i < len(a); i++ {
		if misc.Compare(a[i], max) == 1 {
			merged = append(merged, max)
			break
		}
		merged = append(merged, a[i])
	}

	if len(merged) < 1 {
		return Multiple()
	}

	return Multiple(merged...)
}

// Evaluator evaluates the sharding result.
type Evaluator interface {
	Not() Evaluator
	// Eval evaluates the sharding result.
	Eval(vtab *rule.VTable) (*rule.Shards, error)
}

type emptyEvaluator struct{}

func (e emptyEvaluator) Not() Evaluator {
	return _noopEvaluator
}

func (e emptyEvaluator) String() string {
	return "NONE"
}

func (e emptyEvaluator) Eval(_ *rule.VTable) (*rule.Shards, error) {
	return rule.NewShards(), nil
}

type staticEvaluator rule.Shards

func (s *staticEvaluator) Not() Evaluator {
	return _noopEvaluator
}

func (s *staticEvaluator) String() string {
	return (*rule.Shards)(s).String()
}

func (s *staticEvaluator) Eval(_ *rule.VTable) (*rule.Shards, error) {
	return (*rule.Shards)(s), nil
}

type noopEvaluator struct{}

func (n noopEvaluator) Not() Evaluator {
	return n
}

func (n noopEvaluator) String() string {
	return "FULL" // Infinity
}

func (n noopEvaluator) Eval(_ *rule.VTable) (*rule.Shards, error) {
	return nil, nil
}

type KeyedEvaluator struct {
	k  string
	op cmp.Comparison
	v  proto.Value
}

func (t *KeyedEvaluator) toComparative(metadata *rule.ShardMetadata) *cmp.Comparative {
	var (
		s string
		k cmp.Kind
	)

	if t.v == nil {
		return nil
	}

	switch t.v.Family() {
	case proto.ValueFamilyString:
		k = cmp.Kstring
		s = t.v.String()
	case proto.ValueFamilyTime:
		k = cmp.Kdate
		dt, _ := t.v.Time()
		s = dt.Format("2006-01-02 15:04:05")
	case proto.ValueFamilyDecimal, proto.ValueFamilySign, proto.ValueFamilyUnsigned, proto.ValueFamilyFloat, proto.ValueFamilyBool:
		k = cmp.Kint
		s = t.v.String()
	}

	if metadata != nil {
		switch metadata.Stepper.U {
		case rule.Umonth, rule.Uyear, rule.Uweek, rule.Uday, rule.Uhour:
			k = cmp.Kdate
		case rule.Unum:
			k = cmp.Kint
		case rule.Ustr:
			k = cmp.Kstring
		}
	}

	return cmp.New(t.k, t.op, s, k)
}

func (t *KeyedEvaluator) String() string {
	return fmt.Sprintf("%s %s %v", t.k, t.op, t.v)
}

func (t *KeyedEvaluator) ToLogical() logical.Logical {
	// NOTICE: sort the logical operations, eg: a > 1 AND a > 2 AND a < 1 AND a < 2
	var suffix string
	if t.v != nil {
		suffix = t.v.String()
	}
	return logical.New(t.String(), logical.WithValue(t), logical.WithSortKey(fmt.Sprintf("%s|%d|%s", t.k, t.op, suffix)))
}

func (t *KeyedEvaluator) Eval(vt *rule.VTable) (*rule.Shards, error) {
	var actualMetadata *rule.ShardMetadata

	dbMetadata, tbMetadata, ok := vt.GetShardMetadata(t.k)
	if !ok || (dbMetadata == nil && tbMetadata == nil) {
		return nil, errors.Wrapf(ErrNoRuleMetadata, "cannot get rule metadata %s.%s", vt.Name(), t.k)
	}

	if dbMetadata == tbMetadata || tbMetadata != nil {
		actualMetadata = tbMetadata
	} else {
		actualMetadata = dbMetadata
	}

	mat, err := Route(vt, t.toComparative(actualMetadata))
	if err != nil {
		return nil, err
	}
	it, err := mat.Eval()
	if err != nil {
		return nil, err
	}
	if it == nil {
		return nil, nil
	}
	return MatchTables(vt, t.k, it)
}

func (t *KeyedEvaluator) Not() Evaluator {
	var op cmp.Comparison
	switch t.op {
	case cmp.Cgt:
		op = cmp.Clte
	case cmp.Cgte:
		op = cmp.Clt
	case cmp.Clt:
		op = cmp.Cgte
	case cmp.Clte:
		op = cmp.Clt
	case cmp.Ceq:
		op = cmp.Cne
	case cmp.Cne:
		op = cmp.Ceq
	default:
		panic("unreachable")
	}

	ret := &KeyedEvaluator{}
	*ret = *t
	ret.op = op

	return ret
}

func EvalWithVTable(l logical.Logical, vtab *rule.VTable) (Evaluator, error) {
	ret, err := logical.Eval(l, func(a, b interface{}) (interface{}, error) {
		x := a.(Evaluator)
		y := b.(Evaluator)
		z, err := and(vtab, x, y)
		if err == nil {
			return z, nil
		}
		if errors.Is(err, ErrNoRuleMetadata) {
			return _noopEvaluator, nil
		}
		return nil, errors.WithStack(err)
	}, func(a, b interface{}) (interface{}, error) {
		x := a.(Evaluator)
		y := b.(Evaluator)
		z, err := or(vtab, x, y)

		if err == nil {
			return z, nil
		}
		if errors.Is(err, ErrNoRuleMetadata) {
			return _noopEvaluator, nil
		}
		return nil, errors.WithStack(err)
	}, func(i interface{}) interface{} {
		x := i.(Evaluator)
		return x.Not()
	})
	if err != nil {
		return nil, err
	}
	return ret.(Evaluator), nil
}

func Eval(l logical.Logical, vtab *rule.VTable) (Evaluator, error) {
	ret, err := logical.Eval(l, func(a, b interface{}) (interface{}, error) {
		x := a.(Evaluator)
		y := b.(Evaluator)
		z, err := and(vtab, x, y)
		if err == nil {
			return z, nil
		}
		if errors.Is(err, ErrNoRuleMetadata) {
			return _noopEvaluator, nil
		}
		return nil, errors.WithStack(err)
	}, func(a, b interface{}) (interface{}, error) {
		x := a.(Evaluator)
		y := b.(Evaluator)
		z, err := or(vtab, x, y)

		if err == nil {
			return z, nil
		}
		if errors.Is(err, ErrNoRuleMetadata) {
			return _noopEvaluator, nil
		}
		return nil, errors.WithStack(err)
	}, func(i interface{}) interface{} {
		x := i.(Evaluator)
		return x.Not()
	})
	if err != nil {
		return nil, err
	}
	return ret.(Evaluator), nil
}

func or(vt *rule.VTable, first, second Evaluator) (Evaluator, error) {
	if first == _noopEvaluator || second == _noopEvaluator {
		return _noopEvaluator, nil
	}
	if first == _emptyEvaluator {
		return second, nil
	}
	if second == _emptyEvaluator {
		return first, nil
	}

	switch a := first.(type) {
	case *KeyedEvaluator:
		if !vt.HasColumn(a.k) {
			return _noopEvaluator, nil
		}
	}

	switch b := second.(type) {
	case *KeyedEvaluator:
		if !vt.HasColumn(b.k) {
			return _noopEvaluator, nil
		}
	}

	v1, err := first.Eval(vt)
	if err != nil {
		return nil, err
	}
	v2, err := second.Eval(vt)
	if err != nil {
		return nil, err
	}

	if v1 == nil && v2 == nil {
		return _noopEvaluator, nil
	}

	union := rule.UnionShards(v1, v2)
	if union.Len() < 1 {
		return _emptyEvaluator, nil
	}

	return (*staticEvaluator)(union), nil
}

func processRange(vt *rule.VTable, begin, end *KeyedEvaluator) (Evaluator, error) {
	dbm1, tbm1, ok := vt.GetShardMetadata(begin.k)
	if !ok {
		return nil, ErrNoRuleMetadata
	}

	dbm2, tbm2, ok := vt.GetShardMetadata(end.k)
	if !ok {
		return nil, ErrNoRuleMetadata
	}

	var m1, m2 *rule.ShardMetadata

	if tbm1 != nil && tbm2 != nil {
		m1, m2 = tbm1, tbm2
	} else if dbm1 != nil && dbm2 != nil {
		m1, m2 = dbm1, dbm2
	} else {
		return nil, errors.Errorf("no available rule metadata found: fields=[%s,%s]", begin.k, end.k)
	}

	// string range is not available, use full-scan instead
	if m1.Stepper.U == rule.Ustr && m2.Stepper.U == rule.Ustr {
		return _noopEvaluator, nil
	}

	mat, err := Route(vt, begin.toComparative(m1))
	if err != nil {
		return nil, err
	}
	beginIt, err := mat.Eval()
	if err != nil {
		return nil, err
	}

	mat, err = Route(vt, end.toComparative(m2))
	if err != nil {
		return nil, err
	}
	endIt, err := mat.Eval()
	if err != nil {
		return nil, err
	}

	if beginIt == nil || endIt == nil {
		return _noopEvaluator, nil
	}

	it := toRangeIterator(beginIt, endIt)
	dt, err := MatchTables(vt, begin.k, it)
	if err != nil {
		return nil, err
	}

	if dt == nil {
		return _noopEvaluator, nil
	}

	if dt.Len() < 1 {
		return _emptyEvaluator, nil
	}

	return (*staticEvaluator)(dt), nil
}

func and(vtab *rule.VTable, first, second Evaluator) (Evaluator, error) {
	if first == _emptyEvaluator || second == _emptyEvaluator {
		return _emptyEvaluator, nil
	}

	if first == _noopEvaluator {
		return second, nil
	}
	if second == _noopEvaluator {
		return first, nil
	}

	k1, ok1 := first.(*KeyedEvaluator)
	k2, ok2 := second.(*KeyedEvaluator)

	if ok1 && ok2 {
		if k1.k == k2.k { // same key, handle comparison.
			var rangeMode int8 // 0:
			switch k1.op {
			case cmp.Ceq:
				switch k2.op {
				case cmp.Ceq:
					// a = 1 AND a = 1 => a = 1
					if k1.v == k2.v {
						return k1, nil
					}
					// a = 1 AND a = 2 => always false
					return _emptyEvaluator, nil
				case cmp.Cne:
					// a = 1 AND a <> 1 => always false
					if k1.v == k2.v {
						return _emptyEvaluator, nil
					}
					// a = 1 AND a <> 2 => a = 1
					return k1, nil
				case cmp.Clt:
					switch misc.Compare(k2.v, k1.v) {
					case -1, 0:
						// a = 1 AND a < 1 => always false
						return _emptyEvaluator, nil
					default:
						// a = 1 AND a < 2 => a = 1
						return k1, nil
					}
				case cmp.Clte:
					switch misc.Compare(k2.v, k1.v) {
					case -1:
						// a = 2 AND a <= 1 => always false
						return _emptyEvaluator, nil
					default:
						// a = 2 AND a <= 2 => a = 2
						return k1, nil
					}
				case cmp.Cgt:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a = 1 AND a > 1 => always false
						return _emptyEvaluator, nil
					default:
						// a = 1 AND a > 0 => a = 1
						return k1, nil
					}
				case cmp.Cgte:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a = 1 AND a >= 2 => always false
						return _emptyEvaluator, nil
					default:
						// a = 3 AND a >= 1 => a = 3
						return k1, nil
					}
				}
			case cmp.Cne:
				switch k2.op {
				case cmp.Ceq:
					if k1.v == k2.v { // a <> 1 AND a = 1 -> always false
						return _emptyEvaluator, nil
					}
					// a <> 1 AND a = 2 -> a = 2
					return k2, nil
				}
			case cmp.Clt:
				switch k2.op {
				case cmp.Cne:
					// FIXME: a < 1 AND a <> 2
					return k1, nil
				case cmp.Ceq:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a < 1 AND a = 1 => always false
						return _emptyEvaluator, nil
					default:
						// a < 3 AND a = 1 => a = 1
						return k2, nil
					}
				case cmp.Cgt:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a < 1 AND a > 1 => always false
						return _emptyEvaluator, nil
					default:
						rangeMode = -1
					}
				case cmp.Cgte:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a < 1 AND a >= 2 => always false
						// a < 1 AND a >= 1 => always false
						return _emptyEvaluator, nil
					default:
						rangeMode = -1
					}
				case cmp.Clt:
					switch misc.Compare(k2.v, k1.v) {
					case -1:
						// a < 2 AND a < 1 => a < 1
						return k2, nil
					default:
						// a < 1 AND a < 2 => a < 1
						// a < 1 AND a < 1 => a < 1
						return k1, nil
					}
				case cmp.Clte:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a < 1 AND a <= 2 => a < 1
						// a < 1 AND a <= 1 => a < 1
						return k1, nil
					default:
						// a < 2 AND a <= 1 => a <= 1
						return k2, nil
					}
				}
			case cmp.Clte:
				switch k2.op {
				case cmp.Ceq:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a <= 1 AND a = 2 => always false
						return _emptyEvaluator, nil
					default:
						return k2, nil
					}
				case cmp.Cne:
					// FIXME: a <= 1 AND a <> 2
					return k1, nil
				case cmp.Clt:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a <= 1 AND a < 2 => a <= 1
						return k1, nil
					default:
						// a <= 3 AND a < 3 => a < 3
						// a <= 3 AND a < 1 => a < 1
						return k2, nil
					}
				case cmp.Clte:
					switch misc.Compare(k2.v, k1.v) {
					case -1:
						// a <= 2 AND a <= 1 => a <= 1
						return k2, nil
					default:
						// a <= 2 AND a <= 3 => a <= 2
						// a <= 2 AND a <= 2 => a <= 2
						return k1, nil
					}
				case cmp.Cgt:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a <= 1 AND a > 1 => always false
						return _emptyEvaluator, nil
					default:
						rangeMode = -1
					}
				case cmp.Cgte:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a <= 1 AND a >= 2 => always false
						return _emptyEvaluator, nil
					case 0:
						// a <= 1 AND a >= 1 => a = 1
						return NewKeyed(k1.k, cmp.Ceq, k1.v), nil
					default:
						rangeMode = -1
					}
				}
			case cmp.Cgt:
				switch k2.op {
				case cmp.Ceq:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a > 1 AND a = 2 => a = 2
						return k2, nil
					default:
						// a > 2 AND a = 2 => always false
						// a > 2 AND a = 1 => always false
						return _emptyEvaluator, nil
					}
				case cmp.Cne:
					// FIXME: a > 1 AND a <> 2
					return k1, nil
				case cmp.Cgt:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a > 3 AND a > 4 => a > 4
						return k2, nil
					default:
						// a > 3 AND a > 2 => a > 3
						// a > 3 AND a > 3 => a > 3
						return k1, nil
					}
				case cmp.Cgte:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a > 1 AND a >= 2 => a >= 2
						return k2, nil
					default:
						// a > 1 AND a >= 1 => a > 1
						// a > 3 AND a >= 2 => a > 3
						return k1, nil
					}
				case cmp.Clt:
					switch misc.Compare(k2.v, k1.v) {
					case -1, 0:
						// a > 2 AND b < 1 => always false
						// a > 2 AND b < 2 => always false
						return _emptyEvaluator, nil
					default:
						rangeMode = 1
					}
				case cmp.Clte:
					switch misc.Compare(k2.v, k1.v) {
					case -1, 0:
						// a > 2 AND b <= 1 => always false
						// a > 2 AND b <= 2 => always false
						return _emptyEvaluator, nil
					default:
						rangeMode = 1
					}
				}
			case cmp.Cgte:
				switch k2.op {
				case cmp.Ceq:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a >= 1 AND a = 2 => a = 2
						// a >= 1 AND a = 1 => a = 1
						return k2, nil
					default:
						// a >= 2 AND a = 1 => always false
						return _emptyEvaluator, nil
					}
				case cmp.Cne:
					// FIXME: a >= 1 AND a <> 2
					return k1, nil
				case cmp.Cgt:
					switch misc.Compare(k2.v, k1.v) {
					case 1, 0:
						// a >= 1 AND a > 2 => a > 2
						// a >= 1 AND a > 1 => a > 1
						return k2, nil
					default:
						// a >= 2 AND a > 1 => a >= 2
						return k1, nil
					}
				case cmp.Cgte:
					switch misc.Compare(k2.v, k1.v) {
					case 1:
						// a >= 1 AND a >= 2 => a >= 2
						return k2, nil
					default:
						// a >= 2 AND a >= 1 => a >= 2
						// a >= 2 AND a >= 2 => a >= 2
						return k1, nil
					}
				case cmp.Clt:
					switch misc.Compare(k2.v, k1.v) {
					case -1, 0:
						// a >= 3 AND a < 2 => always false
						// a >= 3 AND a < 3 => always false
						return _emptyEvaluator, nil
					default:
						rangeMode = 1
					}
				case cmp.Clte:
					switch misc.Compare(k2.v, k1.v) {
					case -1:
						// a >= 3 AND a <= 2 => always false
						return _emptyEvaluator, nil
					case 0:
						return NewKeyed(k1.k, cmp.Ceq, k1.v), nil
					default:
						rangeMode = 1
					}
				}
			}

			switch rangeMode {
			case 1:
				return processRange(vtab, k1, k2)
			case -1:
				return processRange(vtab, k2, k1)
			}
		} else if vtab.HasColumn(k1.k) && vtab.HasColumn(k2.k) {
			// SKIP: multiple sharding keys, goto slow path
		} else if vtab.HasColumn(k1.k) {
			return k1, nil
		} else if vtab.HasColumn(k2.k) {
			return k2, nil
		} else {
			return _noopEvaluator, nil
		}
	}

	// slow path
	v1, err := first.Eval(vtab)
	if err != nil {
		return nil, err
	}
	v2, err := second.Eval(vtab)
	if err != nil {
		return nil, err
	}

	if v1 == nil && v2 == nil {
		return _noopEvaluator, nil
	}

	merged := rule.IntersectionShards(v1, v2)

	if merged.Len() < 1 {
		return _emptyEvaluator, nil
	}

	return (*staticEvaluator)(merged), nil
}

func NewKeyed(key string, op cmp.Comparison, value proto.Value) *KeyedEvaluator {
	return &KeyedEvaluator{
		k:  key,
		op: op,
		v:  value,
	}
}
