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

package ast

import (
	"strings"
)

const (
	_                     SQLType = iota
	SQLTypeSelect                 // SELECT
	SQLTypeDelete                 // DELETE
	SQLTypeUpdate                 // UPDATE
	SQLTypeInsert                 // INSERT
	SQLTypeInsertSelect           // INSERT SELECT
	SQLTypeReplace                // REPLACE
	SQLTypeTruncate               // TRUNCATE
	SQLTypeDropTable              // DROP TABLE
	SQLTypeAlterTable             // ALTER TABLE
	SQLTypeDropIndex              // DROP INDEX
	SQLTypeShowDatabases          // SHOW DATABASES
	SQLTypeShowCollation          // SHOW COLLATION
	SQLTypeShowTables             // SHOW TABLES
	SQLTypeShowOpenTables         // SHOW OPEN TABLES
	SQLTypeShowIndex              // SHOW INDEX
	SQLTypeShowColumns            // SHOW COLUMNS
	SQLTypeShowCreate             // SHOW CREATE
	SQLTypeShowVariables          // SHOW VARIABLES
	SQLTypeShowTopology           // SHOW TOPOLOGY
	SQLTypeDescribe               // DESCRIBE
	SQLTypeUnion                  // UNION
	SQLTypeDropTrigger            // DROP TRIGGER
	SQLTypeCreateIndex            // CREATE INDEX
	SQLTypeShowStatus             // SHOW STATUS
)

type RestoreFlag uint32

const (
	RestoreDefault      RestoreFlag = 0
	RestoreLowerKeyword RestoreFlag = 1 << iota // force use lower-case keyword
)

type Restorer interface {
	Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error
}

var _sqlTypeNames = [...]string{
	SQLTypeSelect:         "SELECT",
	SQLTypeDelete:         "DELETE",
	SQLTypeUpdate:         "UPDATE",
	SQLTypeInsert:         "INSERT",
	SQLTypeInsertSelect:   "INSERT SELECT",
	SQLTypeReplace:        "REPLACE",
	SQLTypeTruncate:       "TRUNCATE",
	SQLTypeDropTable:      "DROP TABLE",
	SQLTypeAlterTable:     "ALTER TABLE",
	SQLTypeDropIndex:      "DROP INDEX",
	SQLTypeShowDatabases:  "SHOW DATABASES",
	SQLTypeShowTables:     "SHOW TABLES",
	SQLTypeShowOpenTables: "SHOW OPEN TABLES",
	SQLTypeShowIndex:      "SHOW INDEX",
	SQLTypeShowColumns:    "SHOW COLUMNS",
	SQLTypeShowCreate:     "SHOW CREATE",
	SQLTypeShowVariables:  "SHOW VARIABLES",
	SQLTypeDescribe:       "DESCRIBE",
	SQLTypeUnion:          "UNION",
	SQLTypeDropTrigger:    "DROP TRIGGER",
	SQLTypeCreateIndex:    "CREATE INDEX",
	SQLTypeShowStatus:     "SHOW STATUS",
}

// SQLType represents the type of SQL.
type SQLType uint8

func (s SQLType) String() string {
	return _sqlTypeNames[s]
}

// Statement represents the SQL statement.
type Statement interface {
	paramsCounter
	Restorer
	// Validate validates the current Statement.
	Validate() error
	// Mode returns the SQLType of current Statement.
	Mode() SQLType
}

type paramsCounter interface {
	// CntParams returns the amount of params.
	CntParams() int
}

type inTablesChecker interface {
	// InTables check whether all columns are in the table list.
	InTables(tables map[string]struct{}) error
}

func RestoreToString(flag RestoreFlag, r Restorer) (string, error) {
	var sb strings.Builder
	if err := r.Restore(flag, &sb, nil); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func MustRestoreToString(flag RestoreFlag, r Restorer) string {
	s, err := RestoreToString(flag, r)
	if err != nil {
		panic(err.Error())
	}
	return s
}

type Null struct{}

func (n Null) String() string {
	return "NULL"
}
