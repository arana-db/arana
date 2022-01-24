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

package mysql

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestDSNParse(t *testing.T) {
	dsn := "admin:123456@tcp"
	_, err := ParseDSN(dsn)
	assert.Error(t, err)
	dsnNoAddr := "admin:123456@127.0.0.1:3306/pass"
	_, err = ParseDSN(dsnNoAddr)
	assert.Error(t, err)
}

func TestTcpDSNParse(t *testing.T) {
	dsn := "admin:123456@tcp/pass"
	cfg, err := ParseDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3306", cfg.Addr)
	assert.Equal(t, "admin", cfg.User)
	assert.Equal(t, "123456", cfg.Passwd)
	assert.Equal(t, "pass", cfg.DBName)
}

func TestUnixDSNParse(t *testing.T) {
	dsn := "admin:123456@unix/pass"
	cfg, err := ParseDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "/tmp/mysql.sock", cfg.Addr)
	assert.Equal(t, "admin", cfg.User)
	assert.Equal(t, "123456", cfg.Passwd)
	assert.Equal(t, "pass", cfg.DBName)
}

func TestSimpleDSNParse(t *testing.T) {
	dsn := "admin:123456@tcp(127.0.0.1:3306)/pass"
	cfg, err := ParseDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3306", cfg.Addr)
	assert.Equal(t, "admin", cfg.User)
	assert.Equal(t, "123456", cfg.Passwd)
	assert.Equal(t, "pass", cfg.DBName)

	dsnNoPort := "admin:123456@tcp(127.0.0.1)/pass"
	cfg, err = ParseDSN(dsnNoPort)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3306", cfg.Addr)
	assert.Equal(t, "admin", cfg.User)
	assert.Equal(t, "123456", cfg.Passwd)
	assert.Equal(t, "pass", cfg.DBName)
}

func TestDSNWithParam(t *testing.T) {
	dsn := "admin:123456@tcp(127.0.0.1:3306)/pass?allowAllFiles=true&allowCleartextPasswords=true"
	cfg, err := ParseDSN(dsn)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3306", cfg.Addr)
	assert.Equal(t, "admin", cfg.User)
	assert.Equal(t, "123456", cfg.Passwd)
	assert.Equal(t, "pass", cfg.DBName)

	assert.True(t, cfg.AllowAllFiles)
	assert.True(t, cfg.AllowCleartextPasswords)
}
