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
	"encoding/binary"
	"io"
	"math"
	"strconv"
	"time"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	mysqlErrors "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
)

var (
	_ proto.KeyedRow = (*BinaryRow)(nil)
	_ proto.KeyedRow = (*TextRow)(nil)
)

type BinaryRow struct {
	fields []proto.Field
	raw    []byte
}

func (bi BinaryRow) Get(name string) (proto.Value, error) {
	idx := -1
	for i, it := range bi.fields {
		if it.Name() == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, errors.Errorf("no such field '%s' found", name)
	}

	dest := make([]proto.Value, len(bi.fields))
	if err := bi.Scan(dest); err != nil {
		return nil, errors.WithStack(err)
	}

	return dest[idx], nil
}

func (bi BinaryRow) Fields() []proto.Field {
	return bi.fields
}

func NewBinaryRow(fields []proto.Field, raw []byte) BinaryRow {
	return BinaryRow{
		fields: fields,
		raw:    raw,
	}
}

func (bi BinaryRow) IsBinary() bool {
	return true
}

func (bi BinaryRow) Length() int {
	return len(bi.raw)
}

func (bi BinaryRow) WriteTo(w io.Writer) (n int64, err error) {
	var wrote int
	wrote, err = w.Write(bi.raw)
	if err != nil {
		return
	}
	n += int64(wrote)
	return
}

func (bi BinaryRow) Scan(dest []proto.Value) error {
	if bi.raw[0] != mysql.OKPacket {
		return mysqlErrors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "read binary rows (%v) failed", bi)
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(dest)+7+2)>>3
	nullMask := bi.raw[1:pos]

	for i := 0; i < len(bi.fields); i++ {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		field := bi.fields[i].(*Field)
		// Convert to byte-coded string
		// TODO Optimize storage space based on the length of data types
		mysqlType, _ := mysql.TypeToMySQL(field.fieldType)
		switch mysql.FieldType(mysqlType) {
		case mysql.FieldTypeNULL:
			dest[i] = nil
			continue

		// Numeric Types
		case mysql.FieldTypeTiny:
			if field.flags&mysql.UnsignedFlag != 0 {
				dest[i] = int64(bi.raw[pos])
			} else {
				dest[i] = int64(int8(bi.raw[pos]))
			}
			pos++
			continue

		case mysql.FieldTypeShort, mysql.FieldTypeYear:
			if field.flags&mysql.UnsignedFlag != 0 {
				dest[i] = int64(binary.LittleEndian.Uint16(bi.raw[pos : pos+2]))
			} else {
				dest[i] = int64(int16(binary.LittleEndian.Uint16(bi.raw[pos : pos+2])))
			}
			pos += 2
			continue

		case mysql.FieldTypeInt24, mysql.FieldTypeLong:
			if field.flags&mysql.UnsignedFlag != 0 {
				dest[i] = int64(binary.LittleEndian.Uint32(bi.raw[pos : pos+4]))
			} else {
				dest[i] = int64(int32(binary.LittleEndian.Uint32(bi.raw[pos : pos+4])))
			}
			pos += 4
			continue

		case mysql.FieldTypeLongLong:
			if field.flags&mysql.UnsignedFlag != 0 {
				val := binary.LittleEndian.Uint64(bi.raw[pos : pos+8])
				if val > math.MaxInt64 {
					dest[i] = uint64ToString(val)
				} else {
					dest[i] = int64(val)
				}
			} else {
				dest[i] = int64(binary.LittleEndian.Uint64(bi.raw[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.FieldTypeFloat:
			dest[i] = math.Float32frombits(binary.LittleEndian.Uint32(bi.raw[pos : pos+4]))
			pos += 4
			continue

		case mysql.FieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(bi.raw[pos : pos+8]))
			pos += 8
			continue

		// Length coded Binary Strings
		case mysql.FieldTypeDecimal, mysql.FieldTypeNewDecimal, mysql.FieldTypeVarChar,
			mysql.FieldTypeBit, mysql.FieldTypeEnum, mysql.FieldTypeSet, mysql.FieldTypeTinyBLOB,
			mysql.FieldTypeMediumBLOB, mysql.FieldTypeLongBLOB, mysql.FieldTypeBLOB,
			mysql.FieldTypeVarString, mysql.FieldTypeString, mysql.FieldTypeGeometry, mysql.FieldTypeJSON:
			var val interface{}
			var isNull bool
			var n int
			var err error
			val, isNull, n, err = readLengthEncodedString(bi.raw[pos:])
			dest[i] = val
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err

		case
			mysql.FieldTypeDate, mysql.FieldTypeNewDate, // Date YYYY-MM-DD
			mysql.FieldTypeTime,                               // Time [-][H]HH:MM:SS[.fractal]
			mysql.FieldTypeTimestamp, mysql.FieldTypeDateTime: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

			num, isNull, n := readLengthEncodedInteger(bi.raw[pos:])
			pos += n

			var val interface{}
			var err error
			switch {
			case isNull:
				dest[i] = nil
				continue
			case field.fieldType == mysql.FieldTypeTime:
				// database/sql does not support an equivalent to TIME, return a string
				var dstlen uint8
				switch decimals := field.decimals; decimals {
				case 0x00, 0x1f:
					dstlen = 8
				case 1, 2, 3, 4, 5, 6:
					dstlen = 8 + 1 + decimals
				default:
					return errors.Errorf("protocol error, illegal decimals architecture.Value %d", field.decimals)
				}
				val, err = formatBinaryTime(bi.raw[pos:pos+int(num)], dstlen)
				dest[i] = val
			default:
				val, err = parseBinaryDateTime(num, bi.raw[pos:], time.Local)
				if err == nil {
					dest[i] = val
					break
				}

				var dstlen uint8
				if field.fieldType == mysql.FieldTypeDate {
					dstlen = 10
				} else {
					switch decimals := field.decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return errors.Errorf("protocol error, illegal decimals architecture.Value %d", field.decimals)
					}
				}
				val, err = formatBinaryDateTime(bi.raw[pos:pos+int(num)], dstlen)
				if err != nil {
					return errors.WithStack(err)
				}
				dest[i] = val
			}

			if err == nil {
				pos += int(num)
				continue
			}

			return err

		// Please report if this happens!
		default:
			return errors.Errorf("unknown field type %d", field.fieldType)
		}
	}

	return nil
}

type TextRow struct {
	fields []proto.Field
	raw    []byte
}

func (te TextRow) Get(name string) (proto.Value, error) {
	idx := -1
	for i, it := range te.fields {
		if it.Name() == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, errors.Errorf("no such field '%s' found", name)
	}

	dest := make([]proto.Value, len(te.fields))
	if err := te.Scan(dest); err != nil {
		return nil, errors.WithStack(err)
	}

	return dest[idx], nil
}

func (te TextRow) Fields() []proto.Field {
	return te.fields
}

func NewTextRow(fields []proto.Field, raw []byte) TextRow {
	return TextRow{
		fields: fields,
		raw:    raw,
	}
}

func (te TextRow) IsBinary() bool {
	return false
}

func (te TextRow) Length() int {
	return len(te.raw)
}

func (te TextRow) WriteTo(w io.Writer) (n int64, err error) {
	var wrote int
	wrote, err = w.Write(te.raw)
	if err != nil {
		return
	}
	n += int64(wrote)
	return
}

func (te TextRow) Scan(dest []proto.Value) error {
	// RowSet Packet
	//var val []byte
	//var isNull bool
	var (
		n      int
		err    error
		pos    int
		isNull bool
	)

	// TODO: support parseTime

	var (
		b   []byte
		loc = time.Local
	)

	for i := 0; i < len(te.fields); i++ {
		b, isNull, n, err = readLengthEncodedString(te.raw[pos:])
		if err != nil {
			return errors.WithStack(err)
		}
		pos += n

		if isNull {
			dest[i] = nil
			continue
		}

		switch te.fields[i].(*Field).fieldType {
		case mysql.FieldTypeString, mysql.FieldTypeVarString, mysql.FieldTypeVarChar:
			dest[i] = string(b)
		case mysql.FieldTypeTiny, mysql.FieldTypeShort, mysql.FieldTypeLong,
			mysql.FieldTypeInt24, mysql.FieldTypeLongLong, mysql.FieldTypeYear:
			if te.fields[i].(*Field).flags&mysql.UnsignedFlag > 0 {
				dest[i], err = strconv.ParseUint(string(b), 10, 64)
			} else {
				dest[i], err = strconv.ParseInt(string(b), 10, 64)
			}
			if err != nil {
				return errors.WithStack(err)
			}
		case mysql.FieldTypeFloat, mysql.FieldTypeDouble, mysql.FieldTypeNewDecimal, mysql.FieldTypeDecimal:
			if dest[i], err = strconv.ParseFloat(string(b), 64); err != nil {
				return errors.WithStack(err)
			}
		case mysql.FieldTypeTimestamp, mysql.FieldTypeDateTime, mysql.FieldTypeDate, mysql.FieldTypeNewDate:
			if dest[i], err = parseDateTime(b, loc); err != nil {
				return errors.WithStack(err)
			}
		default:
			dest[i] = b
		}
	}

	return nil
}
