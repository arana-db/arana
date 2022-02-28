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

func TestMySQLToType(t *testing.T) {
	fieldType, _ := MySQLToType(0, 0)
	assert.Equal(t, FieldTypeDecimal, fieldType)
	fieldType, _ = MySQLToType(0, 32)
	assert.Equal(t, FieldTypeDecimal, fieldType)
	fieldType, _ = MySQLToType(1, 0)
	assert.Equal(t, FieldTypeTiny, fieldType)
	fieldType, _ = MySQLToType(1, 32)
	assert.Equal(t, FieldTypeUint8, fieldType)
	fieldType, _ = MySQLToType(2, 0)
	assert.Equal(t, FieldTypeShort, fieldType)
	fieldType, _ = MySQLToType(2, 32)
	assert.Equal(t, FieldTypeUint16, fieldType)
	fieldType, _ = MySQLToType(3, 0)
	assert.Equal(t, FieldTypeLong, fieldType)
	fieldType, _ = MySQLToType(3, 32)
	assert.Equal(t, FieldTypeUint32, fieldType)
	fieldType, _ = MySQLToType(4, 0)
	assert.Equal(t, FieldTypeFloat, fieldType)
	fieldType, _ = MySQLToType(4, 32)
	assert.Equal(t, FieldTypeFloat, fieldType)
	fieldType, _ = MySQLToType(5, 0)
	assert.Equal(t, FieldTypeDouble, fieldType)
	fieldType, _ = MySQLToType(5, 32)
	assert.Equal(t, FieldTypeDouble, fieldType)
	fieldType, _ = MySQLToType(6, 0)
	assert.Equal(t, FieldTypeNULL, fieldType)
	fieldType, _ = MySQLToType(6, 32)
	assert.Equal(t, FieldTypeNULL, fieldType)
	fieldType, _ = MySQLToType(7, 0)
	assert.Equal(t, FieldTypeTimestamp, fieldType)
	fieldType, _ = MySQLToType(7, 32)
	assert.Equal(t, FieldTypeTimestamp, fieldType)
	fieldType, _ = MySQLToType(8, 0)
	assert.Equal(t, FieldTypeLongLong, fieldType)
	fieldType, _ = MySQLToType(8, 32)
	assert.Equal(t, FieldTypeUint64, fieldType)
	fieldType, _ = MySQLToType(9, 0)
	assert.Equal(t, FieldTypeInt24, fieldType)
	fieldType, _ = MySQLToType(9, 32)
	assert.Equal(t, FieldTypeUint24, fieldType)
	fieldType, _ = MySQLToType(10, 0)
	assert.Equal(t, FieldTypeDate, fieldType)
	fieldType, _ = MySQLToType(10, 32)
	assert.Equal(t, FieldTypeDate, fieldType)
	fieldType, _ = MySQLToType(11, 0)
	assert.Equal(t, FieldTypeTime, fieldType)
	fieldType, _ = MySQLToType(11, 32)
	assert.Equal(t, FieldTypeTime, fieldType)
	fieldType, _ = MySQLToType(12, 0)
	assert.Equal(t, FieldTypeDateTime, fieldType)
	fieldType, _ = MySQLToType(12, 32)
	assert.Equal(t, FieldTypeDateTime, fieldType)
	fieldType, _ = MySQLToType(13, 0)
	assert.Equal(t, FieldTypeYear, fieldType)
	fieldType, _ = MySQLToType(13, 32)
	assert.Equal(t, FieldTypeYear, fieldType)
	fieldType, _ = MySQLToType(14, 0)
	assert.Equal(t, FieldTypeNewDate, fieldType)
	fieldType, _ = MySQLToType(14, 32)
	assert.Equal(t, FieldTypeNewDate, fieldType)
	fieldType, _ = MySQLToType(15, 0)
	assert.Equal(t, FieldTypeVarChar, fieldType)
	fieldType, _ = MySQLToType(15, 32)
	assert.Equal(t, FieldTypeVarChar, fieldType)
	fieldType, _ = MySQLToType(16, 0)
	assert.Equal(t, FieldTypeBit, fieldType)
	fieldType, _ = MySQLToType(16, 32)
	assert.Equal(t, FieldTypeBit, fieldType)
	fieldType, _ = MySQLToType(17, 0)
	assert.Equal(t, FieldTypeTimestamp, fieldType)
	fieldType, _ = MySQLToType(17, 32)
	assert.Equal(t, FieldTypeTimestamp, fieldType)
	fieldType, _ = MySQLToType(18, 0)
	assert.Equal(t, FieldTypeDateTime, fieldType)
	fieldType, _ = MySQLToType(18, 32)
	assert.Equal(t, FieldTypeDateTime, fieldType)
	fieldType, _ = MySQLToType(19, 0)
	assert.Equal(t, FieldTypeTime, fieldType)
	fieldType, _ = MySQLToType(19, 32)
	assert.Equal(t, FieldTypeTime, fieldType)
	fieldType, _ = MySQLToType(245, 0)
	assert.Equal(t, FieldTypeJSON, fieldType)
	fieldType, _ = MySQLToType(245, 32)
	assert.Equal(t, FieldTypeJSON, fieldType)
	fieldType, _ = MySQLToType(246, 0)
	assert.Equal(t, FieldTypeDecimal, fieldType)
	fieldType, _ = MySQLToType(246, 32)
	assert.Equal(t, FieldTypeDecimal, fieldType)
	fieldType, _ = MySQLToType(247, 0)
	assert.Equal(t, FieldTypeEnum, fieldType)
	fieldType, _ = MySQLToType(247, 32)
	assert.Equal(t, FieldTypeEnum, fieldType)
	fieldType, _ = MySQLToType(248, 0)
	assert.Equal(t, FieldTypeSet, fieldType)
	fieldType, _ = MySQLToType(248, 32)
	assert.Equal(t, FieldTypeSet, fieldType)
	fieldType, _ = MySQLToType(249, 0)
	assert.Equal(t, FieldTypeTinyBLOB, fieldType)
	fieldType, _ = MySQLToType(249, 32)
	assert.Equal(t, FieldTypeTinyBLOB, fieldType)
	fieldType, _ = MySQLToType(250, 0)
	assert.Equal(t, FieldTypeMediumBLOB, fieldType)
	fieldType, _ = MySQLToType(250, 32)
	assert.Equal(t, FieldTypeMediumBLOB, fieldType)
	fieldType, _ = MySQLToType(251, 0)
	assert.Equal(t, FieldTypeLongBLOB, fieldType)
	fieldType, _ = MySQLToType(251, 32)
	assert.Equal(t, FieldTypeLongBLOB, fieldType)
	fieldType, _ = MySQLToType(252, 0)
	assert.Equal(t, FieldTypeBLOB, fieldType)
	fieldType, _ = MySQLToType(252, 32)
	assert.Equal(t, FieldTypeBLOB, fieldType)
	fieldType, _ = MySQLToType(253, 0)
	assert.Equal(t, FieldTypeVarString, fieldType)
	fieldType, _ = MySQLToType(253, 32)
	assert.Equal(t, FieldTypeVarString, fieldType)
	fieldType, _ = MySQLToType(254, 0)
	assert.Equal(t, FieldTypeString, fieldType)
	fieldType, _ = MySQLToType(254, 32)
	assert.Equal(t, FieldTypeString, fieldType)
	fieldType, _ = MySQLToType(255, 0)
	assert.Equal(t, FieldTypeGeometry, fieldType)
	fieldType, _ = MySQLToType(255, 32)
	assert.Equal(t, FieldTypeGeometry, fieldType)
}

func TestTypeToMySQL(t *testing.T) {
	mysqlType, flags := TypeToMySQL(FieldTypeTiny)
	assert.Equal(t, int64(1), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeUint8)
	assert.Equal(t, int64(1), mysqlType)
	assert.Equal(t, int64(UnsignedFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeShort)
	assert.Equal(t, int64(2), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeLong)
	assert.Equal(t, int64(3), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeUint32)
	assert.Equal(t, int64(3), mysqlType)
	assert.Equal(t, int64(UnsignedFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeFloat)
	assert.Equal(t, int64(4), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeDouble)
	assert.Equal(t, int64(5), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeNULL)
	assert.Equal(t, int64(6), mysqlType)
	assert.Equal(t, int64(BinaryFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeTimestamp)
	assert.Equal(t, int64(7), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeLongLong)
	assert.Equal(t, int64(8), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeUint64)
	assert.Equal(t, int64(8), mysqlType)
	assert.Equal(t, int64(UnsignedFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeInt24)
	assert.Equal(t, int64(9), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeUint24)
	assert.Equal(t, int64(9), mysqlType)
	assert.Equal(t, int64(UnsignedFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeDate)
	assert.Equal(t, int64(10), mysqlType)
	assert.Equal(t, int64(BinaryFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeTime)
	assert.Equal(t, int64(11), mysqlType)
	assert.Equal(t, int64(BinaryFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeDateTime)
	assert.Equal(t, int64(12), mysqlType)
	assert.Equal(t, int64(BinaryFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeYear)
	assert.Equal(t, int64(13), mysqlType)
	assert.Equal(t, int64(UnsignedFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeBit)
	assert.Equal(t, int64(16), mysqlType)
	assert.Equal(t, int64(UnsignedFlag), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeJSON)
	assert.Equal(t, int64(245), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeDecimal)
	assert.Equal(t, int64(246), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeEnum)
	assert.Equal(t, int64(247), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeSet)
	assert.Equal(t, int64(248), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeTinyBLOB)
	assert.Equal(t, int64(249), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeMediumBLOB)
	assert.Equal(t, int64(250), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeLongBLOB)
	assert.Equal(t, int64(251), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeBLOB)
	assert.Equal(t, int64(252), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeVarString)
	assert.Equal(t, int64(253), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeString)
	assert.Equal(t, int64(254), mysqlType)
	assert.Equal(t, int64(0), flags)
	mysqlType, flags = TypeToMySQL(FieldTypeGeometry)
	assert.Equal(t, int64(255), mysqlType)
	assert.Equal(t, int64(0), flags)
}

func TestHasNotNullFlag(t *testing.T) {
	result := HasNotNullFlag(NotNullFlag)
	assert.True(t, result)
	result = HasNotNullFlag(0)
	assert.False(t, result)
}

func TestHasNoDefaultValueFlag(t *testing.T) {
	result := HasNoDefaultValueFlag(NoDefaultValueFlag)
	assert.True(t, result)
	result = HasNoDefaultValueFlag(0)
	assert.False(t, result)
}

func TestHasAutoIncrementFlag(t *testing.T) {
	result := HasAutoIncrementFlag(AutoIncrementFlag)
	assert.True(t, result)
	result = HasAutoIncrementFlag(0)
	assert.False(t, result)
}

func TestHasUnsignedFlag(t *testing.T) {
	result := HasUnsignedFlag(UnsignedFlag)
	assert.True(t, result)
	result = HasUnsignedFlag(0)
	assert.False(t, result)
}

func TestHasZerofillFlag(t *testing.T) {
	result := HasZerofillFlag(ZerofillFlag)
	assert.True(t, result)
	result = HasZerofillFlag(0)
	assert.False(t, result)
}

func TestHasBinaryFlag(t *testing.T) {
	result := HasBinaryFlag(BinaryFlag)
	assert.True(t, result)
	result = HasBinaryFlag(0)
	assert.False(t, result)
}

func TestHasPriKeyFlag(t *testing.T) {
	result := HasPriKeyFlag(PriKeyFlag)
	assert.True(t, result)
	result = HasPriKeyFlag(0)
	assert.False(t, result)
}

func TestHasUniKeyFlag(t *testing.T) {
	result := HasUniKeyFlag(UniqueKeyFlag)
	assert.True(t, result)
	result = HasUniKeyFlag(0)
	assert.False(t, result)
}

func TestHasMultipleKeyFlag(t *testing.T) {
	result := HasMultipleKeyFlag(MultipleKeyFlag)
	assert.True(t, result)
	result = HasMultipleKeyFlag(0)
	assert.False(t, result)
}

func TestHasTimestampFlag(t *testing.T) {
	result := HasTimestampFlag(TimestampFlag)
	assert.True(t, result)
	result = HasTimestampFlag(0)
	assert.False(t, result)
}

func TestHasOnUpdateNowFlag(t *testing.T) {
	result := HasOnUpdateNowFlag(OnUpdateNowFlag)
	assert.True(t, result)
	result = HasOnUpdateNowFlag(0)
	assert.False(t, result)
}

func TestHasParseToJSONFlag(t *testing.T) {
	result := HasParseToJSONFlag(ParseToJSONFlag)
	assert.True(t, result)
	result = HasParseToJSONFlag(0)
	assert.False(t, result)
}

func TestHasIsBooleanFlag(t *testing.T) {
	result := HasIsBooleanFlag(IsBooleanFlag)
	assert.True(t, result)
	result = HasIsBooleanFlag(0)
	assert.False(t, result)
}

func TestHasPreventNullInsertFlag(t *testing.T) {
	result := HasPreventNullInsertFlag(PreventNullInsertFlag)
	assert.True(t, result)
	result = HasPreventNullInsertFlag(0)
	assert.False(t, result)
}
