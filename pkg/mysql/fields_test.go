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
	unitTests := []struct {
		field     mysql.FieldType
		collation string
		expected  string
	}{
		{mysql.FieldTypeBit, mysql.DefaultCollation, "BIT"},
		{mysql.FieldTypeBLOB, mysql.DefaultCollation, "TEXT"},
		{mysql.FieldTypeBLOB, mysql.BinaryCollation, "BLOB"},
		{mysql.FieldTypeDate, mysql.DefaultCollation, "DATE"},
		{mysql.FieldTypeDateTime, mysql.BinaryCollation, "DATETIME"},
		{mysql.FieldTypeDecimal, mysql.DefaultCollation, "DECIMAL"},
		{mysql.FieldTypeDouble, mysql.DefaultCollation, "DOUBLE"},
		{mysql.FieldTypeEnum, mysql.BinaryCollation, "ENUM"},
		{mysql.FieldTypeFloat, mysql.DefaultCollation, "FLOAT"},
		{mysql.FieldTypeGeometry, mysql.DefaultCollation, "GEOMETRY"},
		{mysql.FieldTypeInt24, mysql.BinaryCollation, "MEDIUMINT"},
		{mysql.FieldTypeJSON, mysql.DefaultCollation, "JSON"},
		{mysql.FieldTypeLong, mysql.DefaultCollation, "INT"},
		{mysql.FieldTypeLongBLOB, mysql.BinaryCollation, "LONGBLOB"},
		{mysql.FieldTypeLongBLOB, mysql.DefaultCollation, "LONGTEXT"},
		{mysql.FieldTypeLongLong, mysql.DefaultCollation, "BIGINT"},
		{mysql.FieldTypeMediumBLOB, mysql.BinaryCollation, "MEDIUMBLOB"},
		{mysql.FieldTypeMediumBLOB, mysql.DefaultCollation, "MEDIUMTEXT"},
		{mysql.FieldTypeNewDate, mysql.DefaultCollation, "DATE"},
		{mysql.FieldTypeNewDecimal, mysql.DefaultCollation, "DECIMAL"},
		{mysql.FieldTypeNULL, mysql.DefaultCollation, "NULL"},
		{mysql.FieldTypeSet, mysql.DefaultCollation, "SET"},
		{mysql.FieldTypeShort, mysql.DefaultCollation, "SMALLINT"},
		{mysql.FieldTypeString, mysql.BinaryCollation, "BINARY"},
		{mysql.FieldTypeString, mysql.DefaultCollation, "CHAR"},
		{mysql.FieldTypeTime, mysql.DefaultCollation, "TIME"},
		{mysql.FieldTypeTimestamp, mysql.DefaultCollation, "TIMESTAMP"},
		{mysql.FieldTypeTiny, mysql.DefaultCollation, "TINYINT"},
		{mysql.FieldTypeTinyBLOB, mysql.BinaryCollation, "TINYBLOB"},
		{mysql.FieldTypeTinyBLOB, mysql.DefaultCollation, "TINYTEXT"},
		{mysql.FieldTypeVarChar, mysql.BinaryCollation, "VARBINARY"},
		{mysql.FieldTypeVarChar, mysql.DefaultCollation, "VARCHAR"},
		{mysql.FieldTypeVarString, mysql.BinaryCollation, "VARBINARY"},
		{mysql.FieldTypeVarString, mysql.DefaultCollation, "VARCHAR"},
		{mysql.FieldTypeYear, mysql.DefaultCollation, "YEAR"},
	}
	for _, unit := range unitTests {
		field := createField(unit.field, mysql.Collations[unit.collation])
		assert.Equal(t, unit.expected, field.TypeDatabaseName())
	}
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
