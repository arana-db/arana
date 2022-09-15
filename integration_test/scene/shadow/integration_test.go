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
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

import (
	"github.com/arana-db/arana/test"
)

type IntegrationSuite struct {
	*test.MySuite
}

func TestSuite(t *testing.T) {
	su := test.NewMySuite(
		test.WithMySQLServerAuth("root", "123456"),
		test.WithMySQLDatabase("employees"),
		test.WithConfig("../integration_test/config/shadow/config.yaml"),
		test.WithScriptPath("../integration_test/scripts/shadow"),
		test.WithTestCasePath("../../testcase/casetest.yaml"),
		// WithDevMode(), // NOTICE: UNCOMMENT IF YOU WANT TO DEBUG LOCAL ARANA SERVER!!!
	)
	suite.Run(t, &IntegrationSuite{su})
}

func (s *IntegrationSuite) TestShadowScene() {
	var (
		db = s.DB()
		t  = s.T()
	)
	tx, err := db.Begin()
	assert.NoError(t, err, "should begin a new tx")

	cases := s.TestCases()
	for _, sqlCase := range cases.QueryRowCases {
		for _, sense := range sqlCase.Sense {
			if strings.Compare(strings.TrimSpace(sense), "shadow") == 0 {
				params := strings.Split(sqlCase.Parameters, ",")
				args := make([]interface{}, 0, len(params))
				for _, param := range params {
					k, _ := test.GetValueByType(param)
					args = append(args, k)
				}

				result := tx.QueryRow(sqlCase.SQL)
				err := sqlCase.ExpectedResult.CompareRow(result)
				assert.NoError(t, err, err)
			}
		}
	}

	for _, sqlCase := range cases.ExShHintCases {
		for _, sense := range sqlCase.Sense {
			if strings.Compare(strings.TrimSpace(sense), "shadow") == 0 {
				params := strings.Split(sqlCase.Parameters, ",")
				args := make([]interface{}, 0, len(params))
				for _, param := range params {
					k, _ := test.GetValueByType(param)
					args = append(args, k)
				}

				result, err := tx.Exec(sqlCase.SQL)
				assert.NoError(t, err, err)
				err = sqlCase.ExpectedResult.CompareRow(result)
				assert.NoError(t, err, err)
			}
		}
	}

	/*
		for _, sqlCase := range cases.ExShRegexCases {
			for _, sense := range sqlCase.Sense {
				if strings.Compare(strings.TrimSpace(sense), "shadow") == 0 {
					params := strings.Split(sqlCase.Parameters, ",")
					args := make([]interface{}, 0, len(params))
					for _, param := range params {
						k, _ := test.GetValueByType(param)
						args = append(args, k)
					}

					result, err := tx.Exec(sqlCase.SQL)
					assert.NoError(t, err, err)
					err = sqlCase.ExpectedResult.CompareRow(result)
					assert.NoError(t, err, err)
				}
			}
		}
	*/
}
