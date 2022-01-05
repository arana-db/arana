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

package xxast

const (
	_        SQLType = iota
	Squery           // QUERY
	Sdelete          // DELETE
	Supdate          // UPDATE
	Sinsert          // INSERT
	Sreplace         // REPLACE
)

// SQLType represents the type of SQL.
type SQLType uint8

func (s SQLType) String() string {
	switch s {
	case Squery:
		return "QUERY"
	case Sdelete:
		return "DELETE"
	case Supdate:
		return "UPDATE"
	case Sinsert:
		return "INSERT"
	case Sreplace:
		return "REPLACE"
	default:
		return "UNKNOWN"
	}
}

// Statement represents the SQL statement.
type Statement interface {
	paramsCounter
	// Validate validates the current Statement.
	Validate() error
	// GetSQLType returns the SQLType of current Statement.
	GetSQLType() SQLType
}

type paramsCounter interface {
	// CntParams returns the amount of params.
	CntParams() int
}

type inTablesChecker interface {
	// InTables check whether all columns are in the table list.
	InTables(tables map[string]struct{}) error
}
