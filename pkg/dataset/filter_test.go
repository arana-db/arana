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
	"io"
	"strings"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
)

func TestFilter(t *testing.T) {
	fields := []proto.Field{
		mysql.NewField("id", consts.FieldTypeLong),
		mysql.NewField("name", consts.FieldTypeVarChar),
		mysql.NewField("gender", consts.FieldTypeLong),
	}
	root := &VirtualDataset{
		Columns: fields,
	}

	for i := int64(0); i < 10; i++ {
		root.Rows = append(root.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(i & 1), // 0=female,1=male
		}))
	}

	filtered := Pipe(root, Filter(func(row proto.Row) bool {
		dest := make([]proto.Value, len(fields))
		_ = row.Scan(dest)
		var gender sql.NullInt64
		_ = gender.Scan(dest[2])
		assert.True(t, gender.Valid)
		return gender.Int64 == 1
	}))

	for {
		next, err := filtered.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		dest := make([]proto.Value, len(fields))
		_ = next.Scan(dest)
		assert.Equal(t, "1", fmt.Sprint(dest[2]))

		t.Logf("id=%v, name=%v, gender=%v\n", dest[0], dest[1], dest[2])
	}

	for i := int64(10); i < 100; i++ {
		root.Rows = append(root.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(i & 1), // 0=female,1=male
		}))
	}

	preFiltered := Pipe(root, FilterPrefix(func(row proto.Row) bool {
		dest := make([]proto.Value, len(fields))
		_ = row.Scan(dest)
		var fkname sql.NullString
		_ = fkname.Scan(dest[1])
		assert.True(t, fkname.Valid)
		return strings.HasPrefix(fkname.String, "fake-name-1")
	}, "fake"))

	for {
		next, err := preFiltered.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		dest := make([]proto.Value, len(fields))
		_ = next.Scan(dest)
		t.Logf("id=%v, name=%v, gender=%v\n", dest[0], dest[1], dest[2])
	}
}
