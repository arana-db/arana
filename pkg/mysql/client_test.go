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
	"github.com/dubbogo/arana/pkg/constants/mysql"
	"github.com/dubbogo/arana/pkg/mysql/errors"
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

func TestParseInitialHandshakePacket(t *testing.T) {
	dsn := "admin:123456@tcp(127.0.0.1:3306)/pass?allowAllFiles=true&allowCleartextPasswords=true"
	cfg, _ := ParseDSN(dsn)
	conn := &BackendConnection{conf: cfg}
	conn.c = newConn(new(mockConn))
	data := make([]byte, 4)
	data[0] = mysql.ErrPacket
	data[1] = 400 & 0xff
	data[2] = 400 >> 8
	data[3] = 65
	_, _, _, err := conn.parseInitialHandshakePacket(data)
	assert.Error(t, err)
	assert.Equal(t, "immediate error from server errorCode=400 errorMsg=A", err.(*errors.SQLError).Message)

	data[0] = mysql.ProtocolVersion - 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Error(t, err)

	data = make([]byte, 2)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no server version", err.(*errors.SQLError).Message)

	data = make([]byte, 6)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no connection id", err.(*errors.SQLError).Message)

	data = make([]byte, 14)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no auth-plugin-Content-part-1", err.(*errors.SQLError).Message)

	data = make([]byte, 15)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no filler", err.(*errors.SQLError).Message)

	data = make([]byte, 16)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no capability flags (lower 2 bytes)", err.(*errors.SQLError).Message)

	data = make([]byte, 18)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	length, _, _, _ := conn.parseInitialHandshakePacket(data)
	assert.Equal(t, uint32(0), length)

	data = make([]byte, 19)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no status flags", err.(*errors.SQLError).Message)

	data = make([]byte, 21)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no capability flags (upper 2 bytes)", err.(*errors.SQLError).Message)

	data = make([]byte, 23)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, _, err = conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "parseInitialHandshakePacket: packet has no length of auth-plugin-Content filler", err.(*errors.SQLError).Message)

	data = make([]byte, 24)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	_, _, password, _ := conn.parseInitialHandshakePacket(data)
	assert.Equal(t, mysql.MysqlNativePassword, password)

	data = make([]byte, 37)
	data[0] = mysql.ProtocolVersion
	data[1] = 1
	data[17] = 255
	data[21] = 255
	data[23] = 9
	data[35] = 65
	_, authData, authName, _ := conn.parseInitialHandshakePacket(data)
	assert.Equal(t, "A", authName)
	assert.Equal(t, "\x00\x00\x00\x00\x00\x00\x00\x00", string(authData))
}
