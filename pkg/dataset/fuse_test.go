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
)

func TestFuse(t *testing.T) {
	fields := []proto.Field{
		mysql.NewField("id", consts.FieldTypeLong),
		mysql.NewField("name", consts.FieldTypeVarChar),
	}

	generate := func(offset, length int) proto.Dataset {
		d := &VirtualDataset{
			Columns: fields,
		}

		for i := offset; i < offset+length; i++ {
			d.Rows = append(d.Rows, rows.NewTextVirtualRow(fields, []proto.Value{
				int64(i),
				fmt.Sprintf("fake-name-%d", i),
			}))
		}

		return d
	}

	fuse, err := Fuse(
		func() (proto.Dataset, error) {
			return generate(0, 3), nil
		},
		func() (proto.Dataset, error) {
			return generate(3, 4), nil
		},
		func() (proto.Dataset, error) {
			return generate(7, 3), nil
		},
	)

	assert.NoError(t, err)

	var seq int
	for {
		next, err := fuse.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		values := make([]proto.Value, len(fields))
		_ = next.Scan(values)
		assert.Equal(t, fmt.Sprint(seq), fmt.Sprint(values[0]))

		t.Logf("next: id=%v, name=%v\n", values[0], values[1])

		seq++
	}
}
