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

package dataset

import (
	"database/sql"
	"fmt"
	"sort"
	"testing"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	vrows "github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/rand2"
)

type sortBy struct {
	rows [][]proto.Value
	less func(a, b []proto.Value) bool
}

func (s sortBy) Len() int {
	return len(s.rows)
}

func (s sortBy) Less(i, j int) bool {
	return s.less(s.rows[i], s.rows[j])
}

func (s sortBy) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

type fakeReducer struct {
	fields []proto.Field
	gender sql.NullInt64
	cnt    int64
}

func (fa *fakeReducer) Reduce(next proto.Row) error {
	if !fa.gender.Valid {
		gender, _ := next.(proto.KeyedRow).Get("gender")
		fa.gender.Int64, _ = gender.Int64()
		fa.gender.Valid = true
	}
	fa.cnt++
	return nil
}

func (fa *fakeReducer) Row() proto.Row {
	return vrows.NewTextVirtualRow(fa.fields, []proto.Value{
		proto.NewValueInt64(fa.gender.Int64),
		proto.NewValueInt64(fa.cnt),
	})
}

func TestGroupReduce(t *testing.T) {
	fields := []proto.Field{
		mysql.NewField("id", consts.FieldTypeLong),
		mysql.NewField("name", consts.FieldTypeVarChar),
		mysql.NewField("gender", consts.FieldTypeLong),
	}

	var origin VirtualDataset
	origin.Columns = fields

	var rows [][]proto.Value
	for i := 0; i < 1000; i++ {
		rows = append(rows, []proto.Value{
			proto.NewValueInt64(int64(i)),
			proto.NewValueString(fmt.Sprintf("Fake %d", i)),
			proto.NewValueInt64(rand2.Int63n(2)),
		})
	}

	s := sortBy{
		rows: rows,
		less: func(a, b []proto.Value) bool {
			x, _ := a[2].Int64()
			y, _ := b[2].Int64()
			return x < y
		},
	}
	sort.Sort(s)

	for _, it := range rows {
		origin.Rows = append(origin.Rows, vrows.NewTextVirtualRow(fields, it))
	}

	actualFields := []proto.Field{
		fields[2],
		mysql.NewField("amount", consts.FieldTypeLong),
	}

	// Simulate: SELECT gender,COUNT(*) AS amount FROM xxx WHERE ... GROUP BY gender
	groups := []OrderByItem{{"gender", true}}
	p := Pipe(&origin,
		GroupReduce(
			groups,
			func(fields []proto.Field) []proto.Field {
				return actualFields
			},
			func() Reducer {
				return &fakeReducer{
					fields: actualFields,
				}
			},
		),
	)

	for {
		next, err := p.Next()
		if err != nil {
			break
		}
		v := make([]proto.Value, len(actualFields))
		_ = next.Scan(v)
		t.Logf("next: gender=%v, amount=%v\n", v[0], v[1])
	}
}
