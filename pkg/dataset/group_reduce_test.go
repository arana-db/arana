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
		fa.gender.Int64, fa.gender.Valid = gender.(int64), true
	}
	fa.cnt++
	return nil
}

func (fa *fakeReducer) Row() proto.Row {
	return vrows.NewTextVirtualRow(fa.fields, []proto.Value{
		fa.gender,
		fa.cnt,
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
			int64(i),
			fmt.Sprintf("Fake %d", i),
			rand2.Int63n(2),
		})
	}

	s := sortBy{
		rows: rows,
		less: func(a, b []proto.Value) bool {
			return a[2].(int64) < b[2].(int64)
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
	groups := []string{"gender"}
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
