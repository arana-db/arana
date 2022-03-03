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

import (
	"github.com/dubbogo/arana/pkg/constants/mysql"
)

func TestTableName(t *testing.T) {
	field := createDefaultField()
	assert.Equal(t, "t_order", field.TableName())
}

func TestDataBaseName(t *testing.T) {
	field := createDefaultField()
	assert.Equal(t, "db_arana", field.DataBaseName())
}

func TestTypeDatabaseName(t *testing.T) {
	field := createField(mysql.FieldTypeBit, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "BIT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeBLOB, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "TEXT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeBLOB, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "BLOB", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeDate, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "DATE", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeDateTime, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "DATETIME", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeDecimal, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "DECIMAL", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeDouble, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "DOUBLE", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeEnum, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "ENUM", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeFloat, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "FLOAT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeGeometry, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "GEOMETRY", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeInt24, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "MEDIUMINT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeJSON, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "JSON", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeLong, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "INT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeLongBLOB, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "LONGBLOB", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeLongBLOB, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "LONGTEXT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeLongLong, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "BIGINT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeMediumBLOB, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "MEDIUMBLOB", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeMediumBLOB, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "MEDIUMTEXT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeNewDate, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "DATE", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeNewDecimal, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "DECIMAL", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeNULL, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "NULL", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeSet, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "SET", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeShort, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "SMALLINT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeString, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "BINARY", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeString, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "CHAR", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeTime, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "TIME", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeTimestamp, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "TIMESTAMP", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeTiny, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "TINYINT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeTinyBLOB, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "TINYBLOB", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeTinyBLOB, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "TINYTEXT", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeVarChar, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "VARBINARY", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeVarChar, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "VARCHAR", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeVarString, mysql.Collations[mysql.BinaryCollation])
	assert.Equal(t, "VARBINARY", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeVarString, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "VARCHAR", field.TypeDatabaseName())

	field = createField(mysql.FieldTypeYear, mysql.Collations[mysql.DefaultCollation])
	assert.Equal(t, "YEAR", field.TypeDatabaseName())
}

func createField(fd mysql.FieldType, charSet uint16) *Field {
	result := &Field{
		table:     "t_order",
		database:  "db_arana",
		charSet:   charSet,
		fieldType: fd,
	}
	return result
}

func createDefaultField() *Field {
	result := &Field{
		table:    "t_order",
		database: "db_arana",
	}
	return result
}
