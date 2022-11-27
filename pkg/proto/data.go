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

//go:generate mockgen -destination=../../testdata/mock_data.go -package=testdata . Field,Row,KeyedRow,Dataset,Result
package proto

import (
	"fmt"
	"io"
	"reflect"
)

type (
	// Field contains the name and type of column, it follows sql.ColumnType.
	Field interface {
		// Name returns the name or alias of the column.
		Name() string

		// DecimalSize returns the scale and precision of a decimal type.
		// If not applicable or if not supported ok is false.
		DecimalSize() (precision, scale int64, ok bool)

		// ScanType returns a Go type suitable for scanning into using Rows.Scan.
		// If a driver does not support this property ScanType will return
		// the type of empty interface.
		ScanType() reflect.Type

		// Length returns the column type length for variable length column types such
		// as text and binary field types. If the type length is unbounded the value will
		// be math.MaxInt64 (any database limits will still apply).
		// If the column type is not variable length, such as an int, or if not supported
		// by the driver ok is false.
		Length() (length int64, ok bool)

		// Nullable reports whether the column may be null.
		// If a driver does not support this property ok will be false.
		Nullable() (nullable, ok bool)

		// DatabaseTypeName returns the database system name of the column type. If an empty
		// string is returned, then the driver type name is not supported.
		// Consult your driver documentation for a list of driver data types. Length specifiers
		// are not included.
		// Common type names include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL",
		// "INT", and "BIGINT".
		DatabaseTypeName() string
	}

	// Value represents the cell value of Row.
	Value interface{}

	// Row represents a row data from a result set.
	Row interface {
		io.WriterTo
		IsBinary() bool

		// Length returns the length of Row.
		Length() int

		// Scan scans the Row to values.
		Scan(dest []Value) error
	}

	// KeyedRow represents row with fields.
	KeyedRow interface {
		Row
		// Fields returns the fields of row.
		Fields() []Field
		// Get returns the value of column name.
		Get(name string) (Value, error)
	}

	Dataset interface {
		io.Closer

		// Fields returns the fields of Dataset.
		Fields() ([]Field, error)

		// Next returns the next row.
		Next() (Row, error)
	}

	// Result is the result of a query execution.
	Result interface {
		// Dataset returns the Dataset.
		Dataset() (Dataset, error)

		// LastInsertId returns the database's auto-generated ID
		// after, for example, an INSERT into a table with primary
		// key.
		LastInsertId() (uint64, error)

		// RowsAffected returns the number of rows affected by the
		// query.
		RowsAffected() (uint64, error)
	}
)

func PrintValue(input Value) string {
	switch v := input.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprint(input)
	}
}
