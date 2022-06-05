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

package rows

import (
	"bytes"
	"database/sql"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
)

func TestNew(t *testing.T) {
	fields := []proto.Field{
		mysql.NewField("name", consts.FieldTypeString),
		mysql.NewField("uid", consts.FieldTypeLongLong),
		mysql.NewField("created_at", consts.FieldTypeDateTime),
	}

	var (
		row    VirtualRow
		b      bytes.Buffer
		now    = time.Unix(time.Now().Unix(), 0)
		values = []proto.Value{"foobar", int64(1), now}
	)

	t.Run("Binary", func(t *testing.T) {
		b.Reset()

		row = NewBinaryVirtualRow(fields, values)
		_, err := row.WriteTo(&b)
		assert.NoError(t, err)

		br := mysql.NewBinaryRow(fields, b.Bytes())
		cells := make([]proto.Value, len(fields))
		err = br.Scan(cells)
		assert.NoError(t, err)

		var (
			name      sql.NullString
			uid       sql.NullInt64
			createdAt sql.NullTime
		)

		_ = name.Scan(cells[0])
		_ = uid.Scan(cells[1])
		_ = createdAt.Scan(cells[2])

		t.Log("name:", name.String)
		t.Log("uid:", uid.Int64)
		t.Log("created_at:", createdAt.Time)

		assert.Equal(t, "foobar", name.String)
		assert.Equal(t, int64(1), uid.Int64)
		assert.Equal(t, now, createdAt.Time)
	})

	t.Run("Text", func(t *testing.T) {
		b.Reset()

		row = NewTextVirtualRow(fields, values)
		_, err := row.WriteTo(&b)
		assert.NoError(t, err)

		cells := make([]proto.Value, len(fields))

		tr := mysql.NewTextRow(fields, b.Bytes())
		err = tr.Scan(cells)
		assert.NoError(t, err)

		var (
			name      sql.NullString
			uid       sql.NullInt64
			createdAt sql.NullTime
		)

		_ = name.Scan(cells[0])
		_ = uid.Scan(cells[1])
		_ = createdAt.Scan(cells[2])

		t.Log("name:", name.String)
		t.Log("uid:", uid.Int64)
		t.Log("created_at:", createdAt.Time)

		assert.Equal(t, "foobar", name.String)
		assert.Equal(t, int64(1), uid.Int64)
		assert.Equal(t, now, createdAt.Time)
	})

}
