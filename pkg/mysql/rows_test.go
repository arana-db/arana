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

package mysql

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/proto"
)

func TestFields(t *testing.T) {
	row := &Row{
		Content:   createContent(),
		ResultSet: createResultSet(),
	}
	fields := row.Fields()
	assert.Equal(t, 3, len(fields))
	assert.Equal(t, "db_arana", fields[0].DataBaseName())
	assert.Equal(t, "t_order", fields[1].TableName())
	assert.Equal(t, "DECIMAL", fields[2].TypeDatabaseName())
}

func TestData(t *testing.T) {
	row := &Row{
		Content:   createContent(),
		ResultSet: createResultSet(),
	}
	content := row.Data()
	assert.Equal(t, 3, len(content))
	assert.Equal(t, byte('1'), content[0])
	assert.Equal(t, byte('2'), content[1])
	assert.Equal(t, byte('3'), content[2])
}

func TestColumnsForColumnNames(t *testing.T) {
	row := &Row{
		Content:   createContent(),
		ResultSet: createResultSet(),
	}
	columns := row.Columns()
	assert.Equal(t, "t_order.id", columns[0])
	assert.Equal(t, "t_order.order_id", columns[1])
	assert.Equal(t, "t_order.order_amount", columns[2])
}

func TestColumnsForColumns(t *testing.T) {
	row := &Row{
		Content: createContent(),
		ResultSet: &ResultSet{
			Columns:     createColumns(),
			ColumnNames: nil,
		},
	}
	columns := row.Columns()
	assert.Equal(t, "t_order.id", columns[0])
	assert.Equal(t, "t_order.order_id", columns[1])
	assert.Equal(t, "t_order.order_amount", columns[2])
}

func TestDecodeForRow(t *testing.T) {
	row := &Row{
		Content:   createContent(),
		ResultSet: createResultSet(),
	}
	val, err := row.Decode()
	//TODO row.Decode() is empty.
	assert.Nil(t, val)
	assert.Nil(t, err)
}

func TestEncodeForRow(t *testing.T) {
	values := make([]*proto.Value, 0, 3)
	names := []string{
		"id", "order_id", "order_amount",
	}
	fields := createColumns()
	for i := 0; i < 3; i++ {
		values = append(values, &proto.Value{Raw: []byte{byte(i)}, Len: 1})
	}

	row := &Row{}
	r := row.Encode(values, fields, names)
	assert.Equal(t, 3, len(r.Columns()))
	assert.Equal(t, 3, len(r.Fields()))
	assert.Equal(t, []byte{byte(0), byte(1), byte(2)}, r.Data())

}

func createContent() []byte {
	result := []byte{
		'1', '2', '3',
	}
	return result
}

func createResultSet() *ResultSet {

	result := &ResultSet{
		Columns:     createColumns(),
		ColumnNames: createColumnNames(),
	}

	return result
}

func createColumns() []proto.Field {
	result := []proto.Field{
		&Field{
			database:  "db_arana",
			table:     "t_order",
			name:      "id",
			fieldType: mysql.FieldTypeLong,
		}, &Field{
			database:  "db_arana",
			table:     "t_order",
			name:      "order_id",
			fieldType: mysql.FieldTypeLong,
		}, &Field{
			database:  "db_arana",
			table:     "t_order",
			name:      "order_amount",
			fieldType: mysql.FieldTypeDecimal,
		},
	}
	return result
}

func createColumnNames() []string {
	result := []string{
		"t_order.id", "t_order.order_id", "t_order.order_amount",
	}
	return result
}
