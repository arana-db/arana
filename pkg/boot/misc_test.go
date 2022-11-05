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

package boot

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestParseDatabaseAndTable(t *testing.T) {
	type tt struct {
		name  string
		db    string
		table string
	}
	for _, it := range []tt{
		{"employee.student", "employee", "student"},
		{"fake-db.fake_table", "fake-db", "fake_table"},
	} {
		t.Run(it.name, func(t *testing.T) {
			db, table, err := parseDatabaseAndTable(it.name)
			assert.NoError(t, err)
			assert.Equal(t, it.db, db)
			assert.Equal(t, it.table, table)
		})
	}
}
