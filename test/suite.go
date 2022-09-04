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

const (
	timeout      = "1s"
	readTimeout  = "3s"
	writeTimeout = "5s"
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

func WithConfig(path string) Option {
	return func(mySuite *MySuite) {
		mySuite.configPath = path
	}
}

func WithBootstrap(path string) Option {
	return func(mySuite *MySuite) {
		mySuite.bootstrapConfig = path
	}
}

func WithScriptPath(path string) Option {
	return func(mySuite *MySuite) {
		mySuite.scriptPath = path
	}
}

func (ms *MySuite) LoadActualDataSetPath(path string) error {
	var msg *Message
	err := LoadYamlConfig(path, &msg)
	if err != nil {
		return err
	}
	ms.actualDataset = msg
	return nil
}

func (ms *MySuite) LoadExpectedDataSetPath(path string) error {
	var msg *Message
	err := LoadYamlConfig(path, &msg)
	if err != nil {
		return err
	}
	ms.expectedDataset = msg
	return nil
}

func WithTestCasePath(path string) Option {
	return func(mySuite *MySuite) {
		var ts *Cases
		err := LoadYamlConfig(path, &ts)
		if err != nil {
			return
		}
		mySuite.cases = ts
	}
}

type MySuite struct {
	suite.Suite

	devMode bool

	username, password, database string
	port                         int

	container *MySQLContainer

	db     *sql.DB
	dbSync sync.Once

	tmpFile, bootstrapConfig, configPath, scriptPath string

	cases           *Cases
	actualDataset   *Message
	expectedDataset *Message
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

func (ms *MySuite) ActualDataset() *Message {
	return ms.actualDataset
}

func (ms *MySuite) ExpectedDataset() *Message {
	return ms.expectedDataset
}

func (ms *MySuite) TestCases() *Cases {
	return ms.cases
}

func (ms *MySuite) DB() *sql.DB {
	ms.dbSync.Do(func() {
		if ms.port < 1 {
			return
		}

		var (
			dsn = fmt.Sprintf(
				"arana:123456@tcp(127.0.0.1:%d)/employees?"+
					"timeout=%s&"+
					"readTimeout=%s&"+
					"writeTimeout=%s&"+
					"parseTime=true&"+
					"loc=Local&"+
					"charset=utf8mb4,utf8",
				ms.port, timeout, readTimeout, writeTimeout)
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

func (ms *MySuite) SetupSuite() {
	if ms.devMode {
		return
	}

	var (
		mt = MySQLContainerTester{
			Username: ms.username,
			Password: ms.password,
			Database: ms.database,

			ScriptPath: ms.scriptPath,
		}
		err error
	)

	ms.container, err = mt.SetupMySQLContainer(context.Background())
	require.NoError(ms.T(), err)

	mysqlAddr := fmt.Sprintf("%s:%d", ms.container.Host, ms.container.Port)

	ms.T().Logf("====== mysql is listening on %s ======\n", mysqlAddr)
	ms.T().Logf("====== arana will listen on 127.0.0.1:%d ======\n", ms.port)

	if ms.configPath == "" {
		ms.configPath = "../conf/config.yaml"
	}
	cfgPath := testdata.Path(ms.configPath)

	err = ms.createConfigFile(cfgPath, ms.container.Host, ms.container.Port)
	require.NoError(ms.T(), err)

	if ms.bootstrapConfig == "" {
		ms.bootstrapConfig = "../conf/bootstrap.yaml"
	}
	go func() {
		_ = os.Setenv(constants.EnvConfigPath, ms.tmpFile)
		start.Run(testdata.Path("../conf/bootstrap.yaml"), "")
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
