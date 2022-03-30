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

package mysql

import (
	"errors"
	"net"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	errors2 "github.com/arana-db/arana/pkg/mysql/errors"
)

var (
	errConnClosed        = errors.New("connection is closed")
	errConnTooManyReads  = errors.New("too many reads")
	errConnTooManyWrites = errors.New("too many writes")
)

type mockConn struct {
	laddr         net.Addr
	raddr         net.Addr
	data          []byte
	written       []byte
	queuedReplies [][]byte
	closed        bool
	read          int
	reads         int
	writes        int
	maxReads      int
	maxWrites     int
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.closed {
		return 0, errConnClosed
	}

	m.reads++
	if m.maxReads > 0 && m.reads > m.maxReads {
		return 0, errConnTooManyReads
	}

	n = copy(b, m.data)
	m.read += n
	return
}
func (m *mockConn) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, errConnClosed
	}

	m.writes++
	if m.maxWrites > 0 && m.writes > m.maxWrites {
		return 0, errConnTooManyWrites
	}

	n = len(b)
	m.written = append(m.written, b...)

	if n > 0 && len(m.queuedReplies) > 0 {
		m.data = m.queuedReplies[0]
		m.queuedReplies = m.queuedReplies[1:]
	}
	return
}
func (m *mockConn) Close() error {
	m.closed = true
	return nil
}
func (m *mockConn) LocalAddr() net.Addr {
	return m.laddr
}
func (m *mockConn) RemoteAddr() net.Addr {
	return m.raddr
}
func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}
func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}
func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestComQuit(t *testing.T) {
	c := newConn(new(mockConn))
	assert.False(t, c.IsClosed())
	err := c.writeComQuit()
	assert.NoError(t, err)
	assert.Equal(t, mysql.ComQuit, int(c.conn.(*mockConn).written[4]))
	c.Close()
	assert.True(t, c.IsClosed())
}

func TestParseErrorPacket(t *testing.T) {
	response := make([]byte, 10)
	response[0] = mysql.ErrPacket
	response[1] = 400 & 0xff
	response[2] = 400 >> 8
	response[3] = 35
	err := ParseErrorPacket(response)
	assert.Equal(t, "\x00\x00\x00\x00\x00", err.(*errors2.SQLError).State)
	assert.Equal(t, "\x00", err.(*errors2.SQLError).Message)
}

func TestWriteOKPacket(t *testing.T) {
	c := newConn(new(mockConn))
	assert.False(t, c.IsClosed())
	err := c.writeOKPacket(1, 1, 0, 0)
	assert.NoError(t, err)
	written := c.conn.(*mockConn).written
	assert.Equal(t, 7, int(written[0]))
	assert.Equal(t, mysql.OKPacket, int(written[4]))
	assert.Equal(t, 1, int(written[5]))
	assert.Equal(t, 1, int(written[6]))
	c.Close()
	assert.True(t, c.IsClosed())
}

func TestWriteOKPacketWithEOFHeader(t *testing.T) {
	c := newConn(new(mockConn))
	assert.False(t, c.IsClosed())
	err := c.writeOKPacketWithEOFHeader(1, 1, 0, 0)
	assert.NoError(t, err)
	written := c.conn.(*mockConn).written
	assert.Equal(t, 7, int(written[0]))
	assert.Equal(t, mysql.EOFPacket, int(written[4]))
	assert.Equal(t, 1, int(written[5]))
	assert.Equal(t, 1, int(written[6]))
	c.Close()
	assert.True(t, c.IsClosed())
}

func TestWriteEOFPacket(t *testing.T) {
	c := newConn(new(mockConn))
	assert.False(t, c.IsClosed())
	err := c.writeEOFPacket(1, 1)
	assert.NoError(t, err)
	written := c.conn.(*mockConn).written
	assert.Equal(t, mysql.EOFPacket, int(written[4]))
	c.Close()
	assert.True(t, c.IsClosed())
}

func TestWriteErrorPacket(t *testing.T) {
	c := newConn(new(mockConn))
	assert.False(t, c.IsClosed())
	err2Send := errors2.NewSQLError(mysql.CRVersionError, mysql.SSUnknownSQLState, "bad protocol version: %v", 0)
	err := c.writeErrorPacketFromError(err2Send)
	assert.NoError(t, err)
	written := c.conn.(*mockConn).written
	assert.Equal(t, 9+len(err2Send.Message), int(written[0]))
	assert.Equal(t, mysql.ErrPacket, int(written[4]))
	code := int(written[5]) + int(written[6])<<8
	assert.Equal(t, mysql.CRVersionError, code)
	assert.Equal(t, mysql.SSUnknownSQLState, string(written[8:13]))
	assert.Equal(t, err2Send.Message, string(written[13:13+len(err2Send.Message)]))
	c.Close()
	assert.True(t, c.IsClosed())
}

func TestParseOKPacket(t *testing.T) {
	response := make([]byte, 7)
	response[1] = 1
	response[2] = 1
	affectedRows, lastInsertID, status, warnings, err := parseOKPacket(response)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0x1), affectedRows)
	assert.Equal(t, uint64(0x1), lastInsertID)
	assert.Equal(t, 0, int(status))
	assert.Equal(t, 0, int(warnings))
}

func TestReadEphemeralPacket(t *testing.T) {
	c := newConn(new(mockConn))
	buf := make([]byte, 10)
	buf[0] = 6
	buf[4] = 1
	c.conn.(*mockConn).data = buf
	response, err := c.readEphemeralPacket()
	assert.NoError(t, err)
	assert.Equal(t, buf[4:], response)
}

func TestReadOnePacket(t *testing.T) {
	c := newConn(new(mockConn))
	buf := make([]byte, 10)
	buf[0] = 6
	buf[4] = 1
	c.conn.(*mockConn).data = buf
	response, err := c.readOnePacket()
	assert.NoError(t, err)
	assert.Equal(t, buf[4:], response)
}

func TestReadPacket(t *testing.T) {
	c := newConn(new(mockConn))
	buf := make([]byte, mysql.MaxPacketSize+3)
	buf[0] = 254
	buf[1] = 255
	buf[2] = 255
	buf[4] = 1
	c.conn.(*mockConn).data = buf
	response, err := c.readPacket()
	assert.NoError(t, err)
	assert.Equal(t, mysql.MaxPacketSize-1, len(response))
}
