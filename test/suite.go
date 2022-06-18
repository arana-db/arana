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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

import (
	"github.com/arana-db/arana/cmd/start"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/util/rand2"
	"github.com/arana-db/arana/testdata"
)

type Option func(*MySuite)

func WithDevMode() Option {
	return func(mySuite *MySuite) {
		mySuite.devMode = true
	}
}

func WithMySQLServerAuth(username, password string) Option {
	return func(mySuite *MySuite) {
		mySuite.username = username
		mySuite.password = password
	}
}

func WithMySQLDatabase(database string) Option {
	return func(mySuite *MySuite) {
		mySuite.database = database
	}
}

type MySuite struct {
	suite.Suite

	devMode bool

	username, password, database string
	port                         int

	container *MySQLContainer

	mysqlDb *sql.DB
	db      *sql.DB
	dbSync  sync.Once

	tmpFile string
}

func NewMySuite(options ...Option) *MySuite {
	ms := &MySuite{
		port: 13306,
	}
	for _, it := range options {
		it(ms)
	}
	return ms
}

func (ms *MySuite) DB() *sql.DB {
	ms.dbSync.Do(func() {
		if ms.port < 1 {
			return
		}

		var (
			dsn = fmt.Sprintf("arana:123456@tcp(127.0.0.1:%d)/employees?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8", ms.port)
			err error
		)

		ms.T().Logf("====== connecting %s ======\n", dsn)

		if ms.db, err = sql.Open("mysql", dsn); err != nil {
			ms.T().Log("connect arana failed:", err.Error())
		}
	})

	if ms.db == nil {
		panic("cannot connect db!")
	}

	return ms.db
}

func (ms *MySuite) MySQLDB(schema string) (*sql.DB, error) {
	var (
		mysqlDsn = fmt.Sprintf("root:123456@tcp(127.0.0.1:%d)/%s?timeout=1s&readTimeout=1s&writeTimeout=1s&parseTime=true&loc=Local&charset=utf8mb4,utf8", ms.container.Port, schema)
		err      error
	)
	ms.T().Logf("====== connecting %s ======\n", mysqlDsn)

	if ms.mysqlDb, err = sql.Open("mysql", mysqlDsn); err != nil {
		ms.T().Log("connect mysql failed:", err.Error())
		return nil, err
	}

	if ms.mysqlDb == nil {
		return nil, errors.New("connect mysql failed")
	}

	return ms.mysqlDb, nil
}

func (ms *MySuite) SetupSuite() {
	if ms.devMode {
		return
	}

	var (
		mt = MySQLContainerTester{
			Username: ms.username,
			Password: ms.password,
			Database: ms.database,
		}
		err error
	)

	ms.container, err = mt.SetupMySQLContainer(context.Background())
	require.NoError(ms.T(), err)

	mysqlAddr := fmt.Sprintf("%s:%d", ms.container.Host, ms.container.Port)

	ms.T().Logf("====== mysql is listening on %s ======\n", mysqlAddr)
	ms.T().Logf("====== arana will listen on 127.0.0.1:%d ======\n", ms.port)

	cfgPath := testdata.Path("../conf/config.yaml")

	err = ms.createConfigFile(cfgPath, ms.container.Host, ms.container.Port)
	require.NoError(ms.T(), err)

	go func() {
		_ = os.Setenv(constants.EnvConfigPath, ms.tmpFile)
		start.Run(testdata.Path("../conf/bootstrap.yaml"))
	}()

	// waiting for arana server started
	time.Sleep(10 * time.Second)
}

func (ms *MySuite) TearDownSuite() {
	if ms.devMode {
		if ms.db != nil {
			_ = ms.db.Close()
		}
		return
	}
	if len(ms.tmpFile) > 0 {
		ms.T().Logf("====== remove temp arana config file: %s ====== \n", ms.tmpFile)
		_ = os.Remove(ms.tmpFile)
	}

	if ms.db != nil {
		_ = ms.db.Close()
	}
	_ = ms.container.Terminate(context.Background())
}

func (ms *MySuite) createConfigFile(cfgPath, host string, port int) error {
	b, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return err
	}

	// random port
	ms.port = 13306 + int(rand2.Int31n(10000))

	// resolve host and ports
	content := strings.
		NewReplacer(
			"arana-mysql", host,
			"13306", strconv.Itoa(ms.port), // arana port
			"3306", strconv.Itoa(port), // mysql port
		).
		Replace(string(b))

	// clone temp config file
	f, err := ioutil.TempFile("", "arana.*.yaml")
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err = f.WriteString(content); err != nil {
		return err
	}

	ms.tmpFile = f.Name()
	ms.T().Logf("====== generate temp arana config: %s ======\n", ms.tmpFile)

	return nil
}
