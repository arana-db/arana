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
	"fmt"
	"strings"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/cmp"
	"github.com/arana-db/arana/pkg/util/misc"
)

func compareValue(prev, next interface{}) int {
	if prev == nil && next == nil {
		return 0
	}

	if prev == nil && next != nil {
		return -1
	}
	if prev != nil && next == nil {
		return 1
	}

	switch x := prev.(type) {
	case int64:
		switch y := next.(type) {
		case int64:
			switch {
			case x < y:
				return -1
			case x > y:
				return 1
			default:
				return 0
			}
		}
	case time.Time:
		switch y := next.(type) {
		case time.Time:
			switch {
			case x.Before(y):
				return -1
			case x.After(y):
				return 1
			default:
				return 0
			}
		}
	}

	return strings.Compare(fmt.Sprint(prev), fmt.Sprint(next))
}

func compareComparativeValue(prev, next *cmp.Comparative) int {
	return innerCompareComparative(prev, next, true)
}

func compareComparative(prev, next *cmp.Comparative) int {
	return innerCompareComparative(prev, next, false)
}

func innerCompareComparative(prev, next *cmp.Comparative, valueOnly bool) int {
	if !valueOnly {
		c := strings.Compare(prev.Key(), next.Key())
		if c != 0 {
			return c
		}
		switch {
		case prev.Comparison() < next.Comparison():
			return -1
		case prev.Comparison() > next.Comparison():
			return 1
		}
	}

	if prev.RawValue() == next.RawValue() {
		return 0
	}

	x, _ := prev.Value()
	y, _ := next.Value()

	if x == nil && y == nil {
		return 0
	}

	if x == nil && y != nil {
		return -1
	}

	if x != nil && y == nil {
		return 1
	}

	switch a := x.(type) {
	case time.Time:
		switch b := y.(type) {
		case time.Time:
			switch {
			case a.Before(b):
				return -1
			case a.After(b):
				return 1
			default:
				return 0
			}
		}
	case int64:
		switch b := y.(type) {
		case int64:
			switch {
			case a < b:
				return -1
			case a > b:
				return 1
			default:
				return 0
			}
		}
	}

	return strings.Compare(prev.RawValue(), next.RawValue())
}

func computeLRange(m *rule.ShardMetadata, begin *cmp.Comparative) (ret []interface{}) {
	if begin.Comparison() == cmp.Ceq {
		ret = []interface{}{begin.MustValue()}
		return
	}

	column := m.GetShardColumn(begin.Key())

	nextInclude := begin.Comparison() == cmp.Cgte
	iter, _ := column.Stepper.Ascend(begin.MustValue(), column.Steps)
	for iter.HasNext() {
		next := iter.Next()
		if nextInclude {
			ret = append(ret, next)
		} else {
			nextInclude = true
		}
	}
	return
}

func computeRRange(m *rule.ShardMetadata, end *cmp.Comparative) (ret []interface{}) {
	if end.Comparison() == cmp.Ceq {
		ret = []interface{}{
			end.MustValue(),
		}
		return
	}

	column := m.GetShardColumn(end.Key())

	nextInclude := end.Comparison() == cmp.Clte
	iter, _ := column.Stepper.Descend(end.MustValue(), column.Steps)
	for iter.HasNext() {
		next := iter.Next()
		if nextInclude {
			ret = append(ret, next)
		} else {
			nextInclude = true
		}
	}
	misc.ReverseSlice(ret)
	return
}

func computeRange(m *rule.ShardMetadata, begin, end *cmp.Comparative) (ret []interface{}) {
	var (
		max          interface{}
		beginInclude = begin.Comparison() == cmp.Cgte
		endInclude   bool
		column       = m.GetShardColumn(begin.Key())
	)

	switch end.Comparison() {
	case cmp.Clte:
		max = end.MustValue()
		endInclude = true
	case cmp.Clt:
		max = end.MustValue()
	}

	iter, _ := column.Stepper.Ascend(begin.MustValue(), column.Steps)
L:
	for iter.HasNext() {
		next := iter.Next()
		switch compareValue(next, max) {
		case -1:
			if beginInclude {
				ret = append(ret, next)
			} else if len(ret) == 0 {
				beginInclude = true
			}
		case 0:
			if !endInclude {
				break L
			}
			if beginInclude {
				ret = append(ret, next)
			} else if len(ret) == 0 {
				beginInclude = true
			}
		case 1:
			break L
		}
	}
	return
}

func searchVShard[T any](table *rule.VTable, groups map[string]T) *rule.VShard {
	vShards := table.GetVShards()

L:
	for i := range vShards {
		// check whether each shard keys contains in groups
		for _, it := range vShards[i].Variables() {
			if _, ok := groups[it]; !ok {
				continue L
			}
		}
		return vShards[i]
	}

	return nil
}
