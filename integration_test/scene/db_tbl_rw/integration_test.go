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

package test

import (
	"strings"
	"testing"
)

import (
	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

import (
	"github.com/arana-db/arana/test"
)

// register mysql
type IntegrationSuite struct {
	*test.MySuite
}

func TestSuite(t *testing.T) {
	su := test.NewMySuite(
		test.WithMySQLServerAuth("root", "123456"),
		test.WithMySQLDatabase("employees"),
		test.WithConfig("../integration_test/config/db_tbl_rw/config.yaml"),
		test.WithScriptPath("../integration_test/scripts/db_tbl_rw"),
		test.WithTestCasePath("../../testcase/casetest.yaml"),
		// WithDevMode(), // NOTICE: UNCOMMENT IF YOU WANT TO DEBUG LOCAL ARANA SERVER!!!
	)
	suite.Run(t, &IntegrationSuite{su})
}

func (s *IntegrationSuite) TestDBTBLRWScene() {
	var (
		db = s.DB()
		t  = s.T()
	)
	// tx, err := db.Begin()
	// assert.NoError(t, err, "should begin a new tx")

	cases := s.TestCases()
	for _, sqlCase := range cases.ExecCases {
		for _, sense := range sqlCase.Sense {
			if strings.Compare(strings.TrimSpace(sense), "db_tbl_rw") == 0 {
				params := strings.Split(sqlCase.Parameters, ",")
				args := make([]interface{}, 0, len(params))
				for _, param := range params {
					k, _ := test.GetValueByType(param)
					args = append(args, k)
				}

				// Execute sql
				result, err := db.Exec(sqlCase.SQL, args...)
				assert.NoError(t, err, "exec not right")
				err = sqlCase.ExpectedResult.CompareRow(result)
				assert.NoError(t, err, err)
			}
		}
	}

	for _, sqlCase := range cases.QueryRowCases {
		for _, sense := range sqlCase.Sense {
			if strings.Compare(strings.TrimSpace(sense), "db_tbl_rw") == 0 {
				params := strings.Split(sqlCase.Parameters, ",")
				args := make([]interface{}, 0, len(params))
				for _, param := range params {
					k, _ := test.GetValueByType(param)
					args = append(args, k)
				}

				result := db.QueryRow(sqlCase.SQL, args...)
				err := sqlCase.ExpectedResult.CompareRow(result)
				assert.NoError(t, err, err)
			}
		}
	}

	for _, sqlCase := range cases.DeleteCases {
		for _, sense := range sqlCase.Sense {
			if strings.Compare(strings.TrimSpace(sense), "db_tbl_rw") == 0 {
				params := strings.Split(sqlCase.Parameters, ",")
				args := make([]interface{}, 0, len(params))
				for _, param := range params {
					k, _ := test.GetValueByType(param)
					args = append(args, k)
				}

				// Execute sql
				result, err := db.Exec(sqlCase.SQL, args...)
				assert.NoError(t, err, "exec not right")
				err = sqlCase.ExpectedResult.CompareRow(result)
				assert.NoError(t, err, err)
			}
		}
	}
}
