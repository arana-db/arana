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

func TestMySQLToType(t *testing.T) {
	unitTests := []struct {
		mysqlType int64
		flags     int64
		expected  FieldType
	}{
		{0, 0, FieldTypeDecimal},
		{0, 32, FieldTypeDecimal},
		{1, 0, FieldTypeTiny},
		{2, 0, FieldTypeShort},
		{3, 0, FieldTypeLong},
		{4, 0, FieldTypeFloat},
		{4, 32, FieldTypeFloat},
		{5, 0, FieldTypeDouble},
		{5, 32, FieldTypeDouble},
		{6, 0, FieldTypeNULL},
		{6, 32, FieldTypeNULL},
		{7, 0, FieldTypeTimestamp},
		{7, 32, FieldTypeTimestamp},
		{8, 0, FieldTypeLongLong},
		{9, 0, FieldTypeInt24},
		{10, 0, FieldTypeDate},
		{10, 32, FieldTypeDate},
		{11, 0, FieldTypeTime},
		{11, 32, FieldTypeTime},
		{12, 0, FieldTypeDateTime},
		{12, 32, FieldTypeDateTime},
		{13, 0, FieldTypeYear},
		{13, 32, FieldTypeYear},
		{14, 0, FieldTypeNewDate},
		{14, 32, FieldTypeNewDate},
		{15, 0, FieldTypeVarChar},
		{15, 32, FieldTypeVarChar},
		{16, 0, FieldTypeBit},
		{16, 32, FieldTypeBit},
		{17, 0, FieldTypeTimestamp},
		{17, 32, FieldTypeTimestamp},
		{18, 0, FieldTypeDateTime},
		{18, 32, FieldTypeDateTime},
		{19, 0, FieldTypeTime},
		{19, 32, FieldTypeTime},
		{245, 0, FieldTypeJSON},
		{245, 32, FieldTypeJSON},
		{246, 0, FieldTypeDecimal},
		{246, 32, FieldTypeDecimal},
		{247, 0, FieldTypeEnum},
		{247, 32, FieldTypeEnum},
		{248, 0, FieldTypeSet},
		{248, 32, FieldTypeSet},
		{249, 0, FieldTypeTinyBLOB},
		{249, 32, FieldTypeTinyBLOB},
		{250, 0, FieldTypeMediumBLOB},
		{250, 32, FieldTypeMediumBLOB},
		{251, 0, FieldTypeLongBLOB},
		{251, 32, FieldTypeLongBLOB},
		{252, 0, FieldTypeBLOB},
		{252, 32, FieldTypeBLOB},
		{253, 0, FieldTypeVarString},
		{253, 32, FieldTypeVarString},
		{254, 0, FieldTypeString},
		{254, 32, FieldTypeString},
		{255, 0, FieldTypeGeometry},
		{255, 32, FieldTypeGeometry},
	}
	for _, unit := range unitTests {
		fieldType, ok := MySQLToType(unit.mysqlType, unit.flags)
		assert.Equal(t, unit.expected, fieldType)
		assert.Nil(t, ok)
	}
}

func TestTypeToMySQL(t *testing.T) {
	unitTests := []struct {
		fieldType         FieldType
		expectedMySQLType int64
		expectedFlags     int64
	}{
		{FieldTypeTiny, int64(1), int64(0)},
		{FieldTypeShort, int64(2), int64(0)},
		{FieldTypeLong, int64(3), int64(0)},
		{FieldTypeFloat, int64(4), int64(0)},
		{FieldTypeDouble, int64(5), int64(0)},
		{FieldTypeNULL, int64(6), int64(BinaryFlag)},
		{FieldTypeTimestamp, int64(7), int64(0)},
		{FieldTypeLongLong, int64(8), int64(0)},
		{FieldTypeInt24, int64(9), int64(0)},
		{FieldTypeDate, int64(10), int64(BinaryFlag)},
		{FieldTypeTime, int64(11), int64(BinaryFlag)},
		{FieldTypeDateTime, int64(12), int64(BinaryFlag)},
		{FieldTypeYear, int64(13), int64(UnsignedFlag)},
		{FieldTypeBit, int64(16), int64(UnsignedFlag)},
		{FieldTypeJSON, int64(245), int64(0)},
		{FieldTypeDecimal, int64(246), int64(0)},
		{FieldTypeEnum, int64(247), int64(0)},
		{FieldTypeSet, int64(248), int64(0)},
		{FieldTypeTinyBLOB, int64(249), int64(0)},
		{FieldTypeMediumBLOB, int64(250), int64(0)},
		{FieldTypeLongBLOB, int64(251), int64(0)},
		{FieldTypeBLOB, int64(252), int64(0)},
		{FieldTypeVarString, int64(253), int64(0)},
		{FieldTypeString, int64(254), int64(0)},
		{FieldTypeGeometry, int64(255), int64(0)},
	}
	for _, unit := range unitTests {
		mysqlType, flags := TypeToMySQL(unit.fieldType)
		assert.Equal(t, unit.expectedMySQLType, mysqlType)
		assert.Equal(t, unit.expectedFlags, flags)
	}
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
