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
	"fmt"
	"testing"

	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/reduce"

	vrows "github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/stretchr/testify/assert"
)

func TestReduce(t *testing.T) {
	fields := []proto.Field{
		mysql.NewField("score", consts.FieldTypeLong),
	}

	var origin VirtualDataset
	origin.Columns = fields

	var rows [][]proto.Value
	for i := 0; i < 10; i++ {
		rows = append(rows, []proto.Value{
			proto.NewValueInt64(int64(i)),
		})
	}

	for _, it := range rows {
		origin.Rows = append(origin.Rows, vrows.NewTextVirtualRow(fields, it))
	}

	totalFields := []proto.Field{
		mysql.NewField("total", consts.FieldTypeLong),
	}

	// Simulate: SELECT sum(score) AS total FROM xxx WHERE ...
	pSum := Pipe(&origin,
		Reduce(
			map[int]reduce.Reducer{
				0: reduce.Sum(),
			},
		),
	)
	for {
		next, err := pSum.Next()
		if err != nil {
			break
		}
		assert.NoError(t, err)
		v := make([]proto.Value, len(totalFields))
		_ = next.Scan(v)
		assert.Equal(t, "45", fmt.Sprint(v[0]))
		t.Logf("next: total=%v\n", v[0])
	}

	maxFields := []proto.Field{
		mysql.NewField("max", consts.FieldTypeLong),
	}

	// Simulate: SELECT max(score) AS max FROM xxx WHERE ...
	pMax := Pipe(&origin,
		Reduce(
			map[int]reduce.Reducer{
				0: reduce.Max(),
			},
		),
	)
	for {
		next, err := pMax.Next()
		if err != nil {
			break
		}
		assert.NoError(t, err)
		v := make([]proto.Value, len(maxFields))
		_ = next.Scan(v)
		assert.Equal(t, "9", fmt.Sprint(v[0]))
		t.Logf("next: max=%v\n", v[0])
	}

	minFields := []proto.Field{
		mysql.NewField("min", consts.FieldTypeLong),
	}

	// Simulate: SELECT min(score) AS min FROM xxx WHERE ...
	pMin := Pipe(&origin,
		Reduce(
			map[int]reduce.Reducer{
				0: reduce.Min(),
			},
		),
	)
	for {
		next, err := pMin.Next()
		if err != nil {
			break
		}
		assert.NoError(t, err)
		v := make([]proto.Value, len(minFields))
		_ = next.Scan(v)
		assert.Equal(t, "0", fmt.Sprint(v[0]))
		t.Logf("next: min=%v\n", v[0])
	}
}
