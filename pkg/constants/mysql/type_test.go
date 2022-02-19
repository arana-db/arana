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
	assert.Equal(t, fieldType, FieldTypeDecimal)
	fieldType, _ = MySQLToType(0, 32)
	assert.Equal(t, fieldType, FieldTypeDecimal)
	fieldType, _ = MySQLToType(1, 0)
	assert.Equal(t, fieldType, FieldTypeTiny)
	fieldType, _ = MySQLToType(1, 32)
	assert.Equal(t, fieldType, FieldTypeUint8)
	fieldType, _ = MySQLToType(2, 0)
	assert.Equal(t, fieldType, FieldTypeShort)
	fieldType, _ = MySQLToType(2, 32)
	assert.Equal(t, fieldType, FieldTypeUint16)
	fieldType, _ = MySQLToType(3, 0)
	assert.Equal(t, fieldType, FieldTypeLong)
	fieldType, _ = MySQLToType(3, 32)
	assert.Equal(t, fieldType, FieldTypeUint32)
	fieldType, _ = MySQLToType(4, 0)
	assert.Equal(t, fieldType, FieldTypeFloat)
	fieldType, _ = MySQLToType(4, 32)
	assert.Equal(t, fieldType, FieldTypeFloat)
	fieldType, _ = MySQLToType(5, 0)
	assert.Equal(t, fieldType, FieldTypeDouble)
	fieldType, _ = MySQLToType(5, 32)
	assert.Equal(t, fieldType, FieldTypeDouble)
	fieldType, _ = MySQLToType(6, 0)
	assert.Equal(t, fieldType, FieldTypeNULL)
	fieldType, _ = MySQLToType(6, 32)
	assert.Equal(t, fieldType, FieldTypeNULL)
	fieldType, _ = MySQLToType(7, 0)
	assert.Equal(t, fieldType, FieldTypeTimestamp)
	fieldType, _ = MySQLToType(7, 32)
	assert.Equal(t, fieldType, FieldTypeTimestamp)
	fieldType, _ = MySQLToType(8, 0)
	assert.Equal(t, fieldType, FieldTypeLongLong)
	fieldType, _ = MySQLToType(8, 32)
	assert.Equal(t, fieldType, FieldTypeUint64)
	fieldType, _ = MySQLToType(9, 0)
	assert.Equal(t, fieldType, FieldTypeInt24)
	fieldType, _ = MySQLToType(9, 32)
	assert.Equal(t, fieldType, FieldTypeUint24)
	fieldType, _ = MySQLToType(10, 0)
	assert.Equal(t, fieldType, FieldTypeDate)
	fieldType, _ = MySQLToType(10, 32)
	assert.Equal(t, fieldType, FieldTypeDate)
	fieldType, _ = MySQLToType(11, 0)
	assert.Equal(t, fieldType, FieldTypeTime)
	fieldType, _ = MySQLToType(11, 32)
	assert.Equal(t, fieldType, FieldTypeTime)
	fieldType, _ = MySQLToType(12, 0)
	assert.Equal(t, fieldType, FieldTypeDateTime)
	fieldType, _ = MySQLToType(12, 32)
	assert.Equal(t, fieldType, FieldTypeDateTime)
	fieldType, _ = MySQLToType(13, 0)
	assert.Equal(t, fieldType, FieldTypeYear)
	fieldType, _ = MySQLToType(13, 32)
	assert.Equal(t, fieldType, FieldTypeYear)
	fieldType, _ = MySQLToType(14, 0)
	assert.Equal(t, fieldType, FieldTypeNewDate)
	fieldType, _ = MySQLToType(14, 32)
	assert.Equal(t, fieldType, FieldTypeNewDate)
	fieldType, _ = MySQLToType(15, 0)
	assert.Equal(t, fieldType, FieldTypeVarChar)
	fieldType, _ = MySQLToType(15, 32)
	assert.Equal(t, fieldType, FieldTypeVarChar)
	fieldType, _ = MySQLToType(16, 0)
	assert.Equal(t, fieldType, FieldTypeBit)
	fieldType, _ = MySQLToType(16, 32)
	assert.Equal(t, fieldType, FieldTypeBit)
	fieldType, _ = MySQLToType(17, 0)
	assert.Equal(t, fieldType, FieldTypeTimestamp)
	fieldType, _ = MySQLToType(17, 32)
	assert.Equal(t, fieldType, FieldTypeTimestamp)
	fieldType, _ = MySQLToType(18, 0)
	assert.Equal(t, fieldType, FieldTypeDateTime)
	fieldType, _ = MySQLToType(18, 32)
	assert.Equal(t, fieldType, FieldTypeDateTime)
	fieldType, _ = MySQLToType(19, 0)
	assert.Equal(t, fieldType, FieldTypeTime)
	fieldType, _ = MySQLToType(19, 32)
	assert.Equal(t, fieldType, FieldTypeTime)
	fieldType, _ = MySQLToType(245, 0)
	assert.Equal(t, fieldType, FieldTypeJSON)
	fieldType, _ = MySQLToType(245, 32)
	assert.Equal(t, fieldType, FieldTypeJSON)
	fieldType, _ = MySQLToType(246, 0)
	assert.Equal(t, fieldType, FieldTypeDecimal)
	fieldType, _ = MySQLToType(246, 32)
	assert.Equal(t, fieldType, FieldTypeDecimal)
	fieldType, _ = MySQLToType(247, 0)
	assert.Equal(t, fieldType, FieldTypeEnum)
	fieldType, _ = MySQLToType(247, 32)
	assert.Equal(t, fieldType, FieldTypeEnum)
	fieldType, _ = MySQLToType(248, 0)
	assert.Equal(t, fieldType, FieldTypeSet)
	fieldType, _ = MySQLToType(248, 32)
	assert.Equal(t, fieldType, FieldTypeSet)
	fieldType, _ = MySQLToType(249, 0)
	assert.Equal(t, fieldType, FieldTypeTinyBLOB)
	fieldType, _ = MySQLToType(249, 32)
	assert.Equal(t, fieldType, FieldTypeTinyBLOB)
	fieldType, _ = MySQLToType(250, 0)
	assert.Equal(t, fieldType, FieldTypeMediumBLOB)
	fieldType, _ = MySQLToType(250, 32)
	assert.Equal(t, fieldType, FieldTypeMediumBLOB)
	fieldType, _ = MySQLToType(251, 0)
	assert.Equal(t, fieldType, FieldTypeLongBLOB)
	fieldType, _ = MySQLToType(251, 32)
	assert.Equal(t, fieldType, FieldTypeLongBLOB)
	fieldType, _ = MySQLToType(252, 0)
	assert.Equal(t, fieldType, FieldTypeBLOB)
	fieldType, _ = MySQLToType(252, 32)
	assert.Equal(t, fieldType, FieldTypeBLOB)
	fieldType, _ = MySQLToType(253, 0)
	assert.Equal(t, fieldType, FieldTypeVarString)
	fieldType, _ = MySQLToType(253, 32)
	assert.Equal(t, fieldType, FieldTypeVarString)
	fieldType, _ = MySQLToType(254, 0)
	assert.Equal(t, fieldType, FieldTypeString)
	fieldType, _ = MySQLToType(254, 32)
	assert.Equal(t, fieldType, FieldTypeString)
	fieldType, _ = MySQLToType(255, 0)
	assert.Equal(t, fieldType, FieldTypeGeometry)
	fieldType, _ = MySQLToType(255, 32)
	assert.Equal(t, fieldType, FieldTypeGeometry)
}

func TestTypeToMySQL(t *testing.T) {
	mysqlType, flags := TypeToMySQL(FieldTypeTiny)
	assert.Equal(t, mysqlType, int64(1))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeUint8)
	assert.Equal(t, mysqlType, int64(1))
	assert.Equal(t, flags, int64(UnsignedFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeShort)
	assert.Equal(t, mysqlType, int64(2))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeLong)
	assert.Equal(t, mysqlType, int64(3))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeUint32)
	assert.Equal(t, mysqlType, int64(3))
	assert.Equal(t, flags, int64(UnsignedFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeFloat)
	assert.Equal(t, mysqlType, int64(4))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeDouble)
	assert.Equal(t, mysqlType, int64(5))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeNULL)
	assert.Equal(t, mysqlType, int64(6))
	assert.Equal(t, flags, int64(BinaryFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeTimestamp)
	assert.Equal(t, mysqlType, int64(7))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeLongLong)
	assert.Equal(t, mysqlType, int64(8))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeUint64)
	assert.Equal(t, mysqlType, int64(8))
	assert.Equal(t, flags, int64(UnsignedFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeInt24)
	assert.Equal(t, mysqlType, int64(9))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeUint24)
	assert.Equal(t, mysqlType, int64(9))
	assert.Equal(t, flags, int64(UnsignedFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeDate)
	assert.Equal(t, mysqlType, int64(10))
	assert.Equal(t, flags, int64(BinaryFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeTime)
	assert.Equal(t, mysqlType, int64(11))
	assert.Equal(t, flags, int64(BinaryFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeDateTime)
	assert.Equal(t, mysqlType, int64(12))
	assert.Equal(t, flags, int64(BinaryFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeYear)
	assert.Equal(t, mysqlType, int64(13))
	assert.Equal(t, flags, int64(UnsignedFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeBit)
	assert.Equal(t, mysqlType, int64(16))
	assert.Equal(t, flags, int64(UnsignedFlag))
	mysqlType, flags = TypeToMySQL(FieldTypeJSON)
	assert.Equal(t, mysqlType, int64(245))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeDecimal)
	assert.Equal(t, mysqlType, int64(246))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeEnum)
	assert.Equal(t, mysqlType, int64(247))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeSet)
	assert.Equal(t, mysqlType, int64(248))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeTinyBLOB)
	assert.Equal(t, mysqlType, int64(249))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeMediumBLOB)
	assert.Equal(t, mysqlType, int64(250))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeLongBLOB)
	assert.Equal(t, mysqlType, int64(251))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeBLOB)
	assert.Equal(t, mysqlType, int64(252))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeVarString)
	assert.Equal(t, mysqlType, int64(253))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeString)
	assert.Equal(t, mysqlType, int64(254))
	assert.Equal(t, flags, int64(0))
	mysqlType, flags = TypeToMySQL(FieldTypeGeometry)
	assert.Equal(t, mysqlType, int64(255))
	assert.Equal(t, flags, int64(0))
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
