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

package test_test

import (
	"database/sql"
	"os"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/test"
)

var (
	db *sql.DB
)

func TestMain(m *testing.M) {
	var err error
	// variable to store function to terminate container
	var terminateContainer func()
	terminateContainer, db, err = test.SetupMySQLContainer()
	// make sure container will be terminated at the end
	defer terminateContainer()
	if err != nil {
		log.Error("Failed to setup MySQL container")
		panic(err)
	}
	os.Exit(m.Run())
}

func TestSelect_Integration(t *testing.T) {
	rows, err := db.Query(`SELECT uid, name, score, nickname FROM student_0001 where uid = ?`, 1)
	assert.NoErrorf(t, err, "select row error: %v", err)

	var uid uint64
	var name string
	var score float64
	var nickname string

	if rows.Next() {
		err = rows.Scan(&uid, &name, &score, &nickname)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", name)
	assert.Equal(t, "nc_scott", nickname)
}
