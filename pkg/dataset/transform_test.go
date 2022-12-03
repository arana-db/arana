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
	"io"
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
	"github.com/arana-db/arana/pkg/util/rand2"
)

func TestTransform(t *testing.T) {
	fields := []proto.Field{
		mysql.NewField("id", consts.FieldTypeLong),
		mysql.NewField("name", consts.FieldTypeVarChar),
		mysql.NewField("level", consts.FieldTypeLong),
	}

	root := &VirtualDataset{
		Columns: fields,
	}

	for i := int64(0); i < 10; i++ {
		root.Rows = append(root.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(rand2.Int63n(10)),
		}))
	}

	transformed := Pipe(root, Map(func(fields []proto.Field) []proto.Field {
		return fields
	}, func(row proto.Row) (proto.Row, error) {
		dest := make([]proto.Value, len(fields))
		_ = row.Scan(dest)
		dest[2] = proto.NewValueInt64(100)
		return rows.NewBinaryVirtualRow(fields, dest), nil
	}))

	for {
		next, err := transformed.Next()
		if err == io.EOF {
			break
		}

		assert.NoError(t, err)

		dest := make([]proto.Value, len(fields))
		_ = next.Scan(dest)

		assert.Equal(t, "100", fmt.Sprint(dest[2]))
	}
}
