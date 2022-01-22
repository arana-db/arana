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
	"errors"
	"net"
	"testing"
	"time"
)

import (
	"github.com/dubbogo/arana/pkg/constants/mysql"
	"github.com/stretchr/testify/assert"
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
	m.data = m.data[n:]
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
