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

package rule

import (
	"encoding/binary"
	stdErrors "errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/dubbogo/arana/pkg/proto/rule"
	"github.com/dubbogo/arana/pkg/runtime/cmp"
	"github.com/dubbogo/arana/pkg/runtime/logical"
	"github.com/dubbogo/arana/pkg/runtime/misc"
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
	_ Evaluator = (staticEvaluator)(nil)
)

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
			break
		}
		merged = append(merged, a[i])
	}

	if len(merged) < 1 {
		return Multiple()
	}

	return Multiple(merged...)
}

var ErrNoRuleMetadata = stdErrors.New("no rule metadata found")

// Evaluator evaluates the sharding result.
type Evaluator interface {
	Not() Evaluator
	// Eval evaluates the sharding result.
	Eval(tableName string, rule *rule.Rule) (rule.DatabaseTables, error)
}

type emptyEvaluator struct{}

func (e emptyEvaluator) Not() Evaluator {
	return _noopEvaluator
}

func (e emptyEvaluator) String() string {
	return "NONE"
}

func (e emptyEvaluator) Eval(_ string, _ *rule.Rule) (rule.DatabaseTables, error) {
	return emptyDatabaseTables, nil
}

type staticEvaluator map[string][]string

func (s staticEvaluator) Not() Evaluator {
	return _noopEvaluator
}

func (s staticEvaluator) String() string {
	return (rule.DatabaseTables)(s).String()
}

func (s staticEvaluator) Eval(_ string, _ *rule.Rule) (rule.DatabaseTables, error) {
	return (rule.DatabaseTables)(s), nil
}

type noopEvaluator struct{}

func (n noopEvaluator) Not() Evaluator {
	return n
}

func (n noopEvaluator) String() string {
	return "FULL" // Infinity
}

func (n noopEvaluator) Eval(_ string, _ *rule.Rule) (rule.DatabaseTables, error) {
	return nil, nil
}

type KeyedEvaluator struct {
	k  string
	op cmp.Comparison
	v  interface{}
}

func (t *KeyedEvaluator) toComparative(metadata *rule.ShardMetadata) *cmp.Comparative {
	var (
		s   string
		k   cmp.Kind
		val = t.v
	)

	// 转换下nil
	if val == nil {
		val = misc.Null{}
	}

	switch v := val.(type) {
	case time.Time:
		s = v.Format("2006-01-02 15:04:05")
		k = cmp.Kdate
	case *time.Time:
		s = v.Format("2006-01-02 15:04:05")
		k = cmp.Kdate
	case string:
		k = cmp.Kstring
		s = v
	case int8:
		s = strconv.FormatInt(int64(v), 10)
		k = cmp.Kint
	case int16:
		s = strconv.FormatInt(int64(v), 10)
		k = cmp.Kint
	case int32:
		s = strconv.FormatInt(int64(v), 10)
		k = cmp.Kint
	case int:
		s = strconv.FormatInt(int64(v), 10)
		k = cmp.Kint
	case int64:
		s = strconv.FormatInt(v, 10)
		k = cmp.Kint
	case uint8:
		s = strconv.FormatUint(uint64(v), 10)
		k = cmp.Kint
	case uint16:
		s = strconv.FormatUint(uint64(v), 10)
		k = cmp.Kint
	case uint32:
		s = strconv.FormatUint(uint64(v), 10)
		k = cmp.Kint
	case uint:
		s = strconv.FormatUint(uint64(v), 10)
		k = cmp.Kint
	case uint64:
		s = strconv.FormatUint(v, 10)
		k = cmp.Kint
	case float32:
		s = strconv.FormatInt(int64(v), 10)
		k = cmp.Kint
	case float64:
		// 非法的值兜底为0, 比如除数为0
		if math.IsNaN(v) || math.IsInf(v, 0) {
			s = "0"
		} else {
			s = strconv.FormatInt(int64(v), 10)
		}
		k = cmp.Kint
	case misc.Null:
		return nil
	default:
		panic(fmt.Sprintf("invalid compare value type %T!", v))
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
	// NOTICE: 逻辑运算重排序, 确保同key的原子, 大于在前, 小于在后, 值小的在前, 值大的在后
	var suffix string
	switch v := t.v.(type) {
	case int8, uint8, int16, uint16, int32, uint32, int, int64:
		suffix = fmt.Sprintf("%016X", v)
	case uint:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v))
		suffix = fmt.Sprintf("%016X", b)
	case uint64:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], v)
		suffix = fmt.Sprintf("%016X", b)
	case time.Time:
		suffix = fmt.Sprintf("%016X", v.Unix())
	default:
		suffix = fmt.Sprintf("%v", t.v)
	}
	return logical.New(t.String(), logical.WithValue(t), logical.WithSortKey(fmt.Sprintf("%s|%d|%s", t.k, t.op, suffix)))
}

func (t *KeyedEvaluator) Eval(tableName string, ru *rule.Rule) (rule.DatabaseTables, error) {
	vt, ok := ru.VTable(tableName)
	if !ok {
		return nil, errors.Errorf("no vtable '%s' found", tableName)
	}

	var actualMetadata *rule.ShardMetadata

	dbMetadata, tbMetadata, ok := vt.GetShardMetadata(t.k)
	if !ok || (dbMetadata == nil && tbMetadata == nil) {
		return nil, errors.Wrapf(ErrNoRuleMetadata, "cannot get rule metadata %s.%s", tableName, t.k)
	}

	if dbMetadata == tbMetadata || tbMetadata != nil {
		actualMetadata = tbMetadata
	} else {
		actualMetadata = dbMetadata
	}

	mat, err := Route(ru, tableName, t.toComparative(actualMetadata))
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
	return MatchTables(ru, tableName, t.k, it)
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

	ret := new(KeyedEvaluator)
	*ret = *t
	ret.op = op
	return ret
}

func Eval(l logical.Logical, tableName string, rule *rule.Rule) (Evaluator, error) {
	ret, err := logical.Eval(l, func(a, b interface{}) (interface{}, error) {
		x := a.(Evaluator)
		y := b.(Evaluator)
		return and(tableName, rule, x, y)
	}, func(a, b interface{}) (interface{}, error) {
		x := a.(Evaluator)
		y := b.(Evaluator)
		return or(tableName, rule, x, y)
	}, func(i interface{}) interface{} {
		x := i.(Evaluator)
		return x.Not()
	})
	if err != nil {
		return nil, err
	}
	return ret.(Evaluator), nil
}

func or(tableName string, rule *rule.Rule, first, second Evaluator) (Evaluator, error) {
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
		if !rule.HasColumn(tableName, a.k) {
			return _noopEvaluator, nil
		}
	}

	switch b := second.(type) {
	case *KeyedEvaluator:
		if !rule.HasColumn(tableName, b.k) {
			return _noopEvaluator, nil
		}
	}

	v1, err := first.Eval(tableName, rule)
	if err != nil {
		return nil, err
	}
	v2, err := second.Eval(tableName, rule)
	if err != nil {
		return nil, err
	}

	union := v1.Or(v2)
	if union.IsFullScan() {
		return _noopEvaluator, nil
	}
	if union.IsEmpty() {
		return _emptyEvaluator, nil
	}

	return staticEvaluator(union), nil
}

func processRange(tableName string, ru *rule.Rule, begin, end *KeyedEvaluator) (Evaluator, error) {
	vt, ok := ru.VTable(tableName)
	if !ok {
		return nil, errors.Errorf("no vtable '%s' found", tableName)
	}

	dbm1, tbm1, ok := vt.GetShardMetadata(begin.k)
	if !ok {
		return nil, errors.Errorf("no rule metadata found: field=%s", begin.k)
	}

	dbm2, tbm2, ok := vt.GetShardMetadata(end.k)
	if !ok {
		return nil, errors.Errorf("no rule metadata found: field=%s", end.k)
	}

	var m1, m2 *rule.ShardMetadata

	if tbm1 != nil && tbm2 != nil {
		m1, m2 = tbm1, tbm2
	} else if dbm1 != nil && dbm2 != nil {
		m1, m2 = dbm1, dbm2
	} else {
		return nil, errors.Errorf("no available rule metadata found: fields=[%s,%s]", begin.k, end.k)
	}

	// 字符串范围比较, 退化为全扫描
	if m1.Stepper.U == rule.Ustr && m2.Stepper.U == rule.Ustr {
		return _noopEvaluator, nil
	}

	mat, err := Route(ru, tableName, begin.toComparative(m1))
	if err != nil {
		return nil, err
	}
	beginIt, err := mat.Eval()
	if err != nil {
		return nil, err
	}

	mat, err = Route(ru, tableName, end.toComparative(m2))
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
	dt, err := MatchTables(ru, tableName, begin.k, it)
	if err != nil {
		return nil, err
	}

	if dt == nil {
		return _noopEvaluator, nil
	}

	if dt.IsEmpty() {
		return _emptyEvaluator, nil
	}

	return staticEvaluator(dt), nil
}

func and(tableName string, rule *rule.Rule, first, second Evaluator) (Evaluator, error) {
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
		if k1.k == k2.k { // Key相同, 执行逻辑展开运算
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
				return processRange(tableName, rule, k1, k2)
			case -1:
				return processRange(tableName, rule, k2, k1)
			}
		} else if rule.HasColumn(tableName, k1.k) && rule.HasColumn(tableName, k2.k) {
			// SKIP: 多个sharding key, 走 slow path
		} else if rule.HasColumn(tableName, k1.k) {
			return k1, nil
		} else if rule.HasColumn(tableName, k2.k) {
			return k2, nil
		} else {
			return _noopEvaluator, nil
		}
	}

	// slow path
	v1, err := first.Eval(tableName, rule)
	if err != nil {
		return nil, err
	}
	v2, err := second.Eval(tableName, rule)
	if err != nil {
		return nil, err
	}

	merged := v1.And(v2)

	if merged == nil {
		return _noopEvaluator, nil
	}

	if merged.IsEmpty() {
		return _emptyEvaluator, nil
	}

	return staticEvaluator(merged), nil
}

func NewKeyed(key string, op cmp.Comparison, value interface{}) *KeyedEvaluator {
	return &KeyedEvaluator{
		k:  key,
		op: op,
		v:  value,
	}
}
