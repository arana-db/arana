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
	"database/sql"
	"reflect"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
)

var (
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	scanTypeNullTime  = reflect.TypeOf(sql.NullTime{})
	scanTypeUint8     = reflect.TypeOf(uint8(0))
	scanTypeUint16    = reflect.TypeOf(uint16(0))
	scanTypeUint32    = reflect.TypeOf(uint32(0))
	scanTypeUint64    = reflect.TypeOf(uint64(0))
	scanTypeRawBytes  = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown   = reflect.TypeOf(new(interface{}))
)

type Field struct {
	table        string
	orgTable     string
	database     string
	name         string
	orgName      string
	length       uint32
	flags        uint
	fieldType    mysql.FieldType
	decimals     byte
	charSet      uint16
	columnLength uint32

	defaultValueLength uint64
	defaultValue       []byte
}

func NewField(name string) *Field {
	return &Field{name: name}
}

func (mf *Field) TableName() string {
	return mf.table
}

func (mf *Field) DataBaseName() string {
	return mf.database
}

func (mf *Field) TypeDatabaseName() string {
	switch mf.fieldType {
	case mysql.FieldTypeBit:
		return "BIT"
	case mysql.FieldTypeBLOB:
		if mf.charSet != mysql.Collations[mysql.BinaryCollation] {
			return "TEXT"
		}
		return "BLOB"
	case mysql.FieldTypeDate:
		return "DATE"
	case mysql.FieldTypeDateTime:
		return "DATETIME"
	case mysql.FieldTypeDecimal:
		return "DECIMAL"
	case mysql.FieldTypeDouble:
		return "DOUBLE"
	case mysql.FieldTypeEnum:
		return "ENUM"
	case mysql.FieldTypeFloat:
		return "FLOAT"
	case mysql.FieldTypeGeometry:
		return "GEOMETRY"
	case mysql.FieldTypeInt24:
		return "MEDIUMINT"
	case mysql.FieldTypeJSON:
		return "JSON"
	case mysql.FieldTypeLong:
		return "INT"
	case mysql.FieldTypeLongBLOB:
		if mf.charSet != mysql.Collations[mysql.BinaryCollation] {
			return "LONGTEXT"
		}
		return "LONGBLOB"
	case mysql.FieldTypeLongLong:
		return "BIGINT"
	case mysql.FieldTypeMediumBLOB:
		if mf.charSet != mysql.Collations[mysql.BinaryCollation] {
			return "MEDIUMTEXT"
		}
		return "MEDIUMBLOB"
	case mysql.FieldTypeNewDate:
		return "DATE"
	case mysql.FieldTypeNewDecimal:
		return "DECIMAL"
	case mysql.FieldTypeNULL:
		return "NULL"
	case mysql.FieldTypeSet:
		return "SET"
	case mysql.FieldTypeShort:
		return "SMALLINT"
	case mysql.FieldTypeString:
		if mf.charSet == mysql.Collations[mysql.BinaryCollation] {
			return "BINARY"
		}
		return "CHAR"
	case mysql.FieldTypeTime:
		return "TIME"
	case mysql.FieldTypeTimestamp:
		return "TIMESTAMP"
	case mysql.FieldTypeTiny:
		return "TINYINT"
	case mysql.FieldTypeTinyBLOB:
		if mf.charSet != mysql.Collations[mysql.BinaryCollation] {
			return "TINYTEXT"
		}
		return "TINYBLOB"
	case mysql.FieldTypeVarChar:
		if mf.charSet == mysql.Collations[mysql.BinaryCollation] {
			return "VARBINARY"
		}
		return "VARCHAR"
	case mysql.FieldTypeVarString:
		if mf.charSet == mysql.Collations[mysql.BinaryCollation] {
			return "VARBINARY"
		}
		return "VARCHAR"
	case mysql.FieldTypeYear:
		return "YEAR"
	default:
		return ""
	}
}

func (mf *Field) scanType() reflect.Type {
	switch mf.fieldType {
	case mysql.FieldTypeTiny:
		if mf.flags&mysql.NotNullFlag != 0 {
			if mf.flags&mysql.UnsignedFlag != 0 {
				return scanTypeUint8
			}
			return scanTypeInt8
		}
		return scanTypeNullInt

	case mysql.FieldTypeShort, mysql.FieldTypeYear:
		if mf.flags&mysql.NotNullFlag != 0 {
			if mf.flags&mysql.UnsignedFlag != 0 {
				return scanTypeUint16
			}
			return scanTypeInt16
		}
		return scanTypeNullInt

	case mysql.FieldTypeInt24, mysql.FieldTypeLong:
		if mf.flags&mysql.NotNullFlag != 0 {
			if mf.flags&mysql.UnsignedFlag != 0 {
				return scanTypeUint32
			}
			return scanTypeInt32
		}
		return scanTypeNullInt

	case mysql.FieldTypeLongLong:
		if mf.flags&mysql.NotNullFlag != 0 {
			if mf.flags&mysql.UnsignedFlag != 0 {
				return scanTypeUint64
			}
			return scanTypeInt64
		}
		return scanTypeNullInt

	case mysql.FieldTypeFloat:
		if mf.flags&mysql.NotNullFlag != 0 {
			return scanTypeFloat32
		}
		return scanTypeNullFloat

	case mysql.FieldTypeDouble:
		if mf.flags&mysql.UnsignedFlag != 0 {
			return scanTypeFloat64
		}
		return scanTypeNullFloat

	case mysql.FieldTypeDecimal, mysql.FieldTypeNewDecimal, mysql.FieldTypeVarChar,
		mysql.FieldTypeBit, mysql.FieldTypeEnum, mysql.FieldTypeSet, mysql.FieldTypeTinyBLOB,
		mysql.FieldTypeMediumBLOB, mysql.FieldTypeLongBLOB, mysql.FieldTypeBLOB,
		mysql.FieldTypeVarString, mysql.FieldTypeString, mysql.FieldTypeGeometry, mysql.FieldTypeJSON,
		mysql.FieldTypeTime:
		return scanTypeRawBytes

	case mysql.FieldTypeDate, mysql.FieldTypeNewDate,
		mysql.FieldTypeTimestamp, mysql.FieldTypeDateTime:
		// NullTime is always returned for more consistent behavior as it can
		// handle both cases of parseTime regardless if the field is nullable.
		return scanTypeNullTime

	default:
		return scanTypeUnknown
	}
}
