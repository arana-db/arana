//
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

package mysql

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestFields(t *testing.T) {
	columns := []Field{
		{
			database: "db_arana",
			table:    "t_order",
			name:     "id",
		}, {
			database: "db_arana",
			table:    "t_order",
			name:     "order_id",
		}, {
			database: "db_arana",
			table:    "t_order",
			name:     "order_amount",
		},
	}
	columnsNames := []string{
		"id", "order_id", "order_amount",
	}

	row := &Row{
		Content: make([]byte, 0),
		ResultSet: &ResultSet{
			Columns:     columns,
			ColumnNames: columnsNames,
		},
	}
	assert.True(t, row == nil)

	result := createResult()
	insertId, err := result.LastInsertId()
	assert.Equal(t, uint64(2000), insertId)
	assert.True(t, err == nil)
}

func TestData(t *testing.T) {
	result := createResult()
	affectedRows, err := result.RowsAffected()
	assert.Equal(t, uint64(10), affectedRows)
	assert.True(t, err == nil)
}
