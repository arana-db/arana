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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

import (
	pmysql "github.com/arana-db/parser/mysql"

	gxnet "github.com/dubbogo/gost/net"

	"github.com/pkg/errors"

	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	err2 "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/third_party/bucketpool"
)

const (
	// connBufferSize is how much we buffer for reading and
	// writing. It is also how much we allocate for ephemeral buffers.
	connBufferSize = 16 * 1024

	// packetHeaderSize is the first four bytes of a packet
	packetHeaderSize int = 4
)

// Constants for how ephemeral buffers were used for reading / writing.
const (
	// ephemeralUnused means the ephemeral buffer is not in use at this
	// moment. This is the default value, and is checked so we don't
	// read or write a packet while one is already used.
	ephemeralUnused = iota

	// ephemeralWrite means we currently in process of writing from  currentEphemeralBuffer
	ephemeralWrite

	// ephemeralRead means we currently in process of reading into currentEphemeralBuffer
	ephemeralRead
)

var mysqlServerFlushDelay = flag.Duration("mysql_server_flush_delay", 100*time.Millisecond, "Delay after which buffered response will flushed to client.")

// bufPool is used to allocate and free buffers in an efficient way.
var bufPool = bucketpool.New(connBufferSize, mysql.MaxPacketSize)

// writersPool is used for pooling bufio.Writer objects.
var writersPool = sync.Pool{New: func() interface{} { return bufio.NewWriterSize(nil, connBufferSize) }}

// Conn is a connection between a client and a server, using the MySQL
// binary protocol. It is built on top of an existing net.Conn, that
// has already been established.
//
// Use Connect on the client side to create a connection.
// Use NewListener to create a server side and listen for connections.
type Conn struct {
	// conn is the underlying network connection.
	// Calling Close() on the Conn will close this connection.
	// If there are any ongoing reads or writes, they may get interrupted.
	conn net.Conn

	// schema is the current database name.
	schema string

	// tenant is the current tenant login.
	tenant string

	// connectionID is set:
	// - at Connect() time for clients, with the value returned by
	// the server.
	// - at accept time for the server.
	connectionID uint32

	// transientVariables represents local transient variables.
	// These variables will always keep sync with backend mysql conns.
	transientVariables map[string]proto.Value

	// closed is set to true when Close() is called on the connection.
	closed *atomic.Bool

	// Packet encoding variables.
	sequence       uint8
	bufferedReader *bufio.Reader

	// Buffered writing has a timer which flushes on inactivity.
	bufMu          sync.Mutex
	bufferedWriter *bufio.Writer
	flushTimer     *time.Timer

	// Keep track of how and of the buffer we allocated for an
	// ephemeral packet on the read and write sides.
	// These fields are used by:
	// - StartEphemeralPacket / writeEphemeralPacket methods for writes.
	// - ReadEphemeralPacket / RecycleReadPacket methods for reads.
	currentEphemeralPolicy int
	// currentEphemeralBuffer for tracking allocated temporary buffer for writes and reads respectively.
	// It can be allocated from bufPool or heap and should be recycled in the same manner.
	currentEphemeralBuffer *[]byte

	// StatusFlags are the status flags we will base our returned flags on.
	// This is a bit field, with values documented in constants.go.
	// An interesting value here would be ServerStatusAutocommit.
	// It is only used by the server. These flags can be changed
	// by Handler methods.
	StatusFlags uint16

	// Capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	Capabilities uint32

	// characterSet is the character set used by the other side of the
	// connection.
	// It is set during the initial handshake.
	// See the values in constants.go.
	characterSet uint8

	serverVersion string
}

// newConn is an internal method to create a Conn. Used by client and server
// side for common creation code.
func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn:               conn,
		closed:             atomic.NewBool(false),
		bufferedReader:     bufio.NewReaderSize(conn, connBufferSize),
		transientVariables: make(map[string]proto.Value),
	}
}

func (c *Conn) ServerVersion() string {
	return c.serverVersion
}

func (c *Conn) CharacterSet() uint8 {
	return c.characterSet
}

func (c *Conn) Schema() string {
	return c.schema
}

func (c *Conn) SetSchema(schema string) {
	c.schema = schema
}

func (c *Conn) Tenant() string {
	return c.tenant
}

func (c *Conn) SetTenant(t string) {
	c.tenant = t
}

func (c *Conn) TransientVariables() map[string]proto.Value {
	return c.transientVariables
}

func (c *Conn) SetTransientVariables(v map[string]proto.Value) {
	c.transientVariables = v
}

// startWriterBuffering starts using buffered writes. This should
// be terminated by a call to endWriteBuffering.
func (c *Conn) startWriterBuffering() {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	c.bufferedWriter = writersPool.Get().(*bufio.Writer)
	c.bufferedWriter.Reset(c.conn)
}

// endWriterBuffering must be called to terminate startWriteBuffering.
func (c *Conn) endWriterBuffering() error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter == nil {
		return nil
	}

	defer func() {
		c.bufferedWriter.Reset(nil)
		writersPool.Put(c.bufferedWriter)
		c.bufferedWriter = nil
	}()

	c.stopFlushTimer()
	return c.bufferedWriter.Flush()
}

// getWriter returns the current writer. It may be either
// the original connection or a wrapper. The returned unget
// function must be invoked after the writing is finished.
// In buffered mode, the unget starts a timer to flush any
// buffered Content.
func (c *Conn) getWriter() (w io.Writer, unget func()) {
	c.bufMu.Lock()
	if c.bufferedWriter != nil {
		return c.bufferedWriter, func() {
			c.startFlushTimer()
			c.bufMu.Unlock()
		}
	}
	c.bufMu.Unlock()
	return c.conn, func() {}
}

// startFlushTimer must be called while holding lock on bufMu.
func (c *Conn) startFlushTimer() {
	c.stopFlushTimer()
	c.flushTimer = time.AfterFunc(*mysqlServerFlushDelay, func() {
		c.bufMu.Lock()
		defer c.bufMu.Unlock()

		if c.bufferedWriter == nil {
			return
		}
		c.stopFlushTimer()
		c.bufferedWriter.Flush()
	})
}

// stopFlushTimer must be called while holding lock on bufMu.
func (c *Conn) stopFlushTimer() {
	if c.flushTimer != nil {
		c.flushTimer.Stop()
		c.flushTimer = nil
	}
}

// getReader returns reader for connection. It can be *bufio.Reader or net.Conn
// depending on which buffer size was passed to newServerConn.
func (c *Conn) getReader() io.Reader {
	if c.bufferedReader != nil {
		return c.bufferedReader
	}
	return c.conn
}

func (c *Conn) readHeaderFrom(r io.Reader) (int, error) {
	var header [4]byte
	// Note io.ReadFull will return two different types of errors:
	// 1. if the socket is already closed, and the go runtime knows it,
	//   then ReadFull will return an error (different than EOF),
	//   something like 'read: connection reset by peer'.
	// 2. if the socket is not closed while we start the read,
	//   but gets closed after the read is started, we'll get io.EOF.
	if _, err := io.ReadFull(r, header[:]); err != nil {
		// The special casing of propagating io.EOF up
		// is used by the server side only, to suppress an error
		// message if a client just disconnects.
		if err == io.EOF {
			return 0, err
		}
		if strings.HasSuffix(err.Error(), "read: connection reset by peer") {
			return 0, io.EOF
		}
		return 0, errors.Wrapf(err, "io.ReadFull(header size) failed")
	}

	sequence := uint8(header[3])
	if sequence != c.sequence {
		return 0, errors.Errorf("invalid sequence, expected %v got %v", c.sequence, sequence)
	}

	c.sequence++

	return int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16), nil
}

// readEphemeralPacket attempts to read a packet into buffer.  Do
// not use this method if the contents of the packet needs to be kept
// after the next readEphemeralPacket.
//
// Note if the connection is closed already, an error will be
// returned, and it may not be io.EOF. If the connection closes while
// we are stuck waiting for Content, an error will also be returned, and
// it most likely will be io.EOF.
func (c *Conn) readEphemeralPacket() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(errors.Errorf("readEphemeralPacket: unexpected currentEphemeralPolicy: %v", c.currentEphemeralPolicy))
	}

	r := c.getReader()

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	// Use the bufPool.
	if length < mysql.MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			defer c.recycleReadPacket()
			return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	// Much slower path, revert to allocating everything from scratch.
	// We're going to concatenate a lot of Content anyway, can't really
	// optimize this code path easily.
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < mysql.MaxPacketSize {
			break
		}
	}

	return data, nil
}

// readEphemeralPacketDirect attempts to read a packet from the socket directly.
// It needs to be used for the first handshake packet the server receives,
// so we don't buffer the SSL negotiation packet. As a shortcut, only
// packets smaller than MaxPacketSize can be read here.
// This function usually shouldn't be used - use readEphemeralPacket.
func (c *Conn) readEphemeralPacketDirect() ([]byte, error) {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(fmt.Sprintf("readEphemeralPacketDirect: unexpected currentEphemeralPolicy: %v!", c.currentEphemeralPolicy))
	}

	var r io.Reader = c.conn

	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}

	c.currentEphemeralPolicy = ephemeralRead
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	if length < mysql.MaxPacketSize {
		c.currentEphemeralBuffer = bufPool.Get(length)
		if _, err := io.ReadFull(r, *c.currentEphemeralBuffer); err != nil {
			defer c.recycleReadPacket()
			return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
		}
		return *c.currentEphemeralBuffer, nil
	}

	return nil, errors.Errorf("readEphemeralPacketDirect doesn't support more than one packet")
}

// recycleReadPacket recycles the read packet. It needs to be called
// after readEphemeralPacket was called.
func (c *Conn) recycleReadPacket() {
	if c.currentEphemeralPolicy != ephemeralRead {
		// Programming error.
		panic(fmt.Sprintf("trying to call recycleReadPacket while currentEphemeralPolicy is %d!", c.currentEphemeralPolicy))
	}
	if c.currentEphemeralBuffer != nil {
		// We are using the pool, put the buffer back in.
		bufPool.Put(c.currentEphemeralBuffer)
		c.currentEphemeralBuffer = nil
	}
	c.currentEphemeralPolicy = ephemeralUnused
}

// readOnePacket reads a single packet into a newly allocated buffer.
func (c *Conn) readOnePacket() ([]byte, error) {
	r := c.getReader()
	length, err := c.readHeaderFrom(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		// This can be caused by the packet after a packet of
		// exactly size MaxPacketSize.
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, errors.Wrapf(err, "io.ReadFull(packet body of length %v) failed", length)
	}
	return data, nil
}

// readPacket reads a packet from the underlying connection.
// It re-assembles packets that span more than one message.
// This method returns a generic error, not a SQLError.
func (c *Conn) readPacket() ([]byte, error) {
	// Optimize for a single packet case.
	data, err := c.readOnePacket()
	if err != nil {
		return nil, err
	}

	// This is a single packet.
	if len(data) < mysql.MaxPacketSize {
		return data, nil
	}

	// There is more than one packet, read them all.
	for {
		next, err := c.readOnePacket()
		if err != nil {
			return nil, err
		}

		if len(next) == 0 {
			// Again, the packet after a packet of exactly size MaxPacketSize.
			break
		}

		data = append(data, next...)
		if len(next) < mysql.MaxPacketSize {
			break
		}
	}

	return data, nil
}

// ReadPacket reads a packet from the underlying connection.
// it is the public API version, that returns a SQLError.
// The memory for the packet is always allocated, and it is owned by the caller
// after this function returns.
func (c *Conn) ReadPacket() ([]byte, error) {
	result, err := c.readPacket()
	if err != nil {
		return nil, err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", err)
	}
	return result, err
}

func (c *Conn) writePacketForFieldList(data []byte) error {
	index := 0
	dataLength := len(data) - packetHeaderSize

	w, unget := c.getWriter()
	defer unget()

	var header [packetHeaderSize]byte
	for {
		// toBeSent is capped to MaxPacketSize.
		toBeSent := dataLength
		if toBeSent > mysql.MaxPacketSize {
			toBeSent = mysql.MaxPacketSize
		}

		// Write the body.
		if n, err := w.Write(data[index : index+toBeSent+packetHeaderSize]); err != nil {
			return errors.Wrapf(err, "Write(header) failed")
		} else if n != (toBeSent + packetHeaderSize) {
			return errors.Wrapf(err, "Write(packet) returned a short write: %v < %v", n, toBeSent+packetHeaderSize)
		}

		// Update our state.
		c.sequence++
		dataLength -= toBeSent
		if dataLength == 0 {
			if toBeSent == mysql.MaxPacketSize {
				// The packet we just sent had exactly
				// MaxPacketSize size, we need to
				// send a zero-size packet too.
				header[0] = 0
				header[1] = 0
				header[2] = 0
				header[3] = c.sequence
				if n, err := w.Write(data[index : index+toBeSent+packetHeaderSize]); err != nil {
					return errors.Wrapf(err, "Write(header) failed")
				} else if n != (toBeSent + packetHeaderSize) {
					return errors.Wrapf(err, "Write(packet) returned a short write: %v < %v", n, (toBeSent + packetHeaderSize))
				}
				c.sequence++
			}
			return nil
		}
		index += toBeSent
	}
}

// writePacket writes a packet, possibly cutting it into multiple
// chunks.  Note this is not very efficient, as the client probably
// has to build the []byte and that makes a memory copy.
// Try to use startEphemeralPacket/writeEphemeralPacket instead.
//
// This method returns a generic error, not a SQLError.
func (c *Conn) writePacket(data []byte) error {
	if err := gxnet.ConnCheck(c.conn); err != nil {
		return errors.Errorf("Check conn status error")
	}
	index := 0
	length := len(data)

	w, unget := c.getWriter()
	defer unget()

	for {
		// Packet length is capped to MaxPacketSize.
		packetLength := length
		if packetLength > mysql.MaxPacketSize {
			packetLength = mysql.MaxPacketSize
		}

		// Compute and write the header.
		var header [4]byte
		header[0] = byte(packetLength)
		header[1] = byte(packetLength >> 8)
		header[2] = byte(packetLength >> 16)
		header[3] = c.sequence
		if n, err := w.Write(header[:]); err != nil {
			return errors.Wrapf(err, "Write(header) failed")
		} else if n != 4 {
			return errors.Errorf("Write(header) returned a short write: %v < 4", n)
		}

		// Write the body.
		if n, err := w.Write(data[index : index+packetLength]); err != nil {
			return errors.Wrapf(err, "Write(packet) failed")
		} else if n != packetLength {
			return errors.Errorf("Write(packet) returned a short write: %v < %v", n, packetLength)
		}

		// Update our state.
		c.sequence++
		length -= packetLength
		if length == 0 {
			if packetLength == mysql.MaxPacketSize {
				// The packet we just sent had exactly
				// MaxPacketSize size, we need to
				// send a zero-size packet too.
				header[0] = 0
				header[1] = 0
				header[2] = 0
				header[3] = c.sequence
				if n, err := w.Write(header[:]); err != nil {
					return errors.Wrapf(err, "Write(empty header) failed")
				} else if n != 4 {
					return errors.Errorf("Write(empty header) returned a short write: %v < 4", n)
				}
				c.sequence++
			}
			return nil
		}
		index += packetLength
	}
}

func (c *Conn) startEphemeralPacket(length int) []byte {
	if c.currentEphemeralPolicy != ephemeralUnused {
		panic(fmt.Sprintf("startEphemeralPacket cannot be used while a packet is already started, actual is %v!", c.currentEphemeralPolicy))
	}

	c.currentEphemeralPolicy = ephemeralWrite
	// get buffer from pool, or it'll be allocated if length is too big
	c.currentEphemeralBuffer = bufPool.Get(length)
	return *c.currentEphemeralBuffer
}

// writeEphemeralPacket writes the packet that was allocated by
// startEphemeralPacket.
func (c *Conn) writeEphemeralPacket() error {
	defer c.recycleWritePacket()

	switch c.currentEphemeralPolicy {
	case ephemeralWrite:
		if err := c.writePacket(*c.currentEphemeralBuffer); err != nil {
			return errors.Wrapf(err, "conn %v", c.ID())
		}
	case ephemeralUnused, ephemeralRead:
		// Programming error.
		panic(fmt.Sprintf("conn %v: trying to call writeEphemeralPacket while currentEphemeralPolicy is %v!", c.ID(), c.currentEphemeralPolicy))
	}

	return nil
}

// recycleWritePacket recycles write packet. It needs to be called
// after writeEphemeralPacket was called.
func (c *Conn) recycleWritePacket() {
	if c.currentEphemeralPolicy != ephemeralWrite {
		// Programming error.
		panic(fmt.Sprintf("trying to call recycleWritePacket while currentEphemeralPolicy is %d!", c.currentEphemeralPolicy))
	}
	// Release our reference so the buffer can be gced
	bufPool.Put(c.currentEphemeralBuffer)
	c.currentEphemeralBuffer = nil
	c.currentEphemeralPolicy = ephemeralUnused
}

// writeComQuit writes a Quit message for the server, to indicate we
// want to close the connection.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) writeComQuit() error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0

	data := c.startEphemeralPacket(1)
	data[0] = mysql.ComQuit
	if err := c.writeEphemeralPacket(); err != nil {
		return err2.NewSQLError(mysql.CRServerGone, mysql.SSUnknownSQLState, err.Error())
	}
	return nil
}

// RemoteAddr returns the underlying socket RemoteAddr().
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// ID returns the MySQL connection ID for this connection.
func (c *Conn) ID() uint32 {
	return c.connectionID
}

// Ident returns a useful identification string for error logging
func (c *Conn) String() string {
	return fmt.Sprintf("client %v (%s)", c.ID(), c.RemoteAddr().String())
}

// Close closes the connection. It can be called from a different go
// routine to interrupt the current connection.
func (c *Conn) Close() {
	if c.closed.CAS(false, true) {
		c.conn.Close()
	}
}

// IsClosed returns true if this connection was ever closed by the
// Close() method.  Note if the other side closes the connection, but
// Close() wasn't called, this will return false.
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

//
// Packet writing methods, for generic packets.
//

// writeOKPacket writes an OK packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacket(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // OKPacket
		lenEncIntSize(affectedRows) +
		lenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, mysql.OKPacket)
	pos = writeLenEncInt(data, pos, affectedRows)
	pos = writeLenEncInt(data, pos, lastInsertID)
	pos = writeUint16(data, pos, flags)
	_ = writeUint16(data, pos, warnings)

	return c.writeEphemeralPacket()
}

// writeOKPacketWithEOFHeader writes an OK packet with an EOF header.
// This is used at the end of a result set if
// CapabilityClientDeprecateEOF is set.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeOKPacketWithEOFHeader(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	length := 1 + // EOFPacket
		lenEncIntSize(affectedRows) +
		lenEncIntSize(lastInsertID) +
		2 + // flags
		2 // warnings
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, mysql.EOFPacket)
	pos = writeLenEncInt(data, pos, affectedRows)
	pos = writeLenEncInt(data, pos, lastInsertID)
	pos = writeUint16(data, pos, flags)
	_ = writeUint16(data, pos, warnings)

	return c.writeEphemeralPacket()
}

func (c *Conn) fixErrNoSuchTable(errorMessage string) string {
	// FIXME: workaround, need refinement in the future.
	// Table 'employees_0000.teacher' doesn't exist
	if matches := getFixErrNoSuchTableRegexp().FindStringSubmatch(errorMessage); len(matches) == 3 {
		var sb strings.Builder
		sb.Grow(len(errorMessage))
		sb.WriteString("Table '")
		sb.WriteString(c.Schema())
		sb.WriteByte('.')
		sb.WriteString(matches[2])
		sb.WriteString("' doesn't exist")
		return sb.String()
	}

	return errorMessage
}

// writeErrorPacket writes an error packet.
// Server -> Client.
// This method returns a generic error, not a SQLError.
func (c *Conn) writeErrorPacket(errorCode uint16, sqlState string, format string, args ...interface{}) error {
	errorMessage := fmt.Sprintf(format, args...)
	switch errorCode {
	case pmysql.ErrNoSuchTable:
		errorMessage = c.fixErrNoSuchTable(errorMessage)
	}

	length := 1 + 2 + 1 + 5 + len(errorMessage)
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, mysql.ErrPacket)
	pos = writeUint16(data, pos, errorCode)
	pos = writeByte(data, pos, '#')
	if sqlState == "" {
		sqlState = mysql.SSUnknownSQLState
	}
	if len(sqlState) != 5 {
		panic(fmt.Sprintf("sqlState has to be 5 characters long, actual is %d!", len(sqlState)))
	}
	pos = writeEOFString(data, pos, sqlState)
	_ = writeEOFString(data, pos, errorMessage)

	return c.writeEphemeralPacket()
}

// writeErrorPacketFromError writes an error packet, from a regular error.
// See writeErrorPacket for other info.
func (c *Conn) writeErrorPacketFromError(err error) error {
	if se, ok := err.(*err2.SQLError); ok {
		return c.writeErrorPacket(uint16(se.Num), se.State, "%v", se.Message)
	}

	return c.writeErrorPacket(mysql.ERUnknownError, mysql.SSUnknownSQLState, "unknown error: %v", err)
}

func (c *Conn) buildEOFPacket(flags uint16, warnings uint16) []byte {
	data := make([]byte, 9)
	pos := 0
	data[pos] = 0x05
	pos += 3
	pos = writeLenEncInt(data, pos, uint64(c.sequence))
	pos = writeByte(data, pos, mysql.EOFPacket)
	pos = writeUint16(data, pos, flags)
	_ = writeUint16(data, pos, warnings)
	return data
}

// writeEOFPacket writes an EOF packet, through the buffer, and
// doesn't flush (as it is used as part of a query result).
func (c *Conn) writeEOFPacket(flags uint16, warnings uint16) error {
	length := 5
	data := c.startEphemeralPacket(length)
	pos := 0
	pos = writeByte(data, pos, mysql.EOFPacket)
	pos = writeUint16(data, pos, warnings)
	_ = writeUint16(data, pos, flags)

	return c.writeEphemeralPacket()
}

//
// Packet parsing methods, for generic packets.
//

// isEOFPacket determines whether a Content packet is a "true" EOF. DO NOT blindly compare the
// first byte of a packet to EOFPacket as you might do for other packet types, as 0xfe is overloaded
// as a first byte.
//
// Per https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html, a packet starting with 0xfe
// but having length >= 9 (on top of 4 byte header) is not a true EOF but a LengthEncodedInteger
// (typically preceding a LengthEncodedString). Thus, all EOF checks must validate the payload size
// before exiting.
//
// More specifically, an EOF packet can have 3 different lengths (1, 5, 7) depending on the client
// flags that are set. 7 comes from server versions of 5.7.5 or greater where ClientDeprecateEOF is
// set (i.e. uses an OK packet starting with 0xfe instead of 0x00 to signal EOF). Regardless, 8 is
// an upper bound otherwise it would be ambiguous w.r.t. LengthEncodedIntegers.
//
// More docs here:
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_response_packets.html
func isEOFPacket(data []byte) bool {
	return data[0] == mysql.EOFPacket && len(data) < 9
}

// parseEOFPacket returns the warning count and a boolean to indicate if there
// are more results to receive.
//
// Note: This is only valid on actual EOF packets and not on OK packets with the EOF
// type code set, i.e. should not be used if ClientDeprecateEOF is set.
func parseEOFPacket(data []byte) (warnings uint16, more bool, err error) {
	// The warning count is in position 2 & 3
	warnings, _, _ = readUint16(data, 1)

	// The status flag is in position 4 & 5
	statusFlags, _, ok := readUint16(data, 3)
	if !ok {
		return 0, false, errors.Errorf("invalid EOF packet statusFlags: %v", data)
	}
	return warnings, (statusFlags & mysql.ServerMoreResultsExists) != 0, nil
}

func parseOKPacket(data []byte) (uint64, uint64, uint16, uint16, error) {
	// We already read the type.
	pos := 1

	// Affected rows.
	affectedRows, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet affectedRows: %v", data)
	}

	// Last Insert ID.
	lastInsertID, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet lastInsertID: %v", data)
	}

	// Status flags.
	statusFlags, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet statusFlags: %v", data)
	}

	// Warnings.
	warnings, _, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, 0, 0, errors.Errorf("invalid OK packet warnings: %v", data)
	}

	return affectedRows, lastInsertID, statusFlags, warnings, nil
}

// isErrorPacket determines whether or not the packet is an error packet. Mostly here for
// consistency with isEOFPacket
func isErrorPacket(data []byte) bool {
	return data[0] == mysql.ErrPacket
}

// ParseErrorPacket parses the error packet and returns a SQLError.
func ParseErrorPacket(data []byte) error {
	// We already read the type.
	pos := 1

	// Error code is 2 bytes.
	code, pos, ok := readUint16(data, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRUnknownError, mysql.SSUnknownSQLState, "invalid error packet code: %v", data)
	}

	// '#' marker of the SQL state is 1 byte. Ignored.
	pos++

	// SQL state is 5 bytes
	sqlState, pos, ok := readBytes(data, pos, 5)
	if !ok {
		return err2.NewSQLError(mysql.CRUnknownError, mysql.SSUnknownSQLState, "invalid error packet sqlState: %v", data)
	}

	// Human-readable error message is the rest.
	msg := string(data[pos:])

	return err2.NewSQLError(int(code), string(sqlState), "%v", msg)
}

// GetTLSClientCerts gets TLS certificates.
func (c *Conn) GetTLSClientCerts() []*x509.Certificate {
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		return tlsConn.ConnectionState().PeerCertificates
	}
	return nil
}

func (c *Conn) GetNetConn() net.Conn {
	return c.conn
}

var (
	_fixErrNoSuchTableRegexp     *regexp.Regexp
	_fixErrNoSuchTableRegexpOnce sync.Once
)

func getFixErrNoSuchTableRegexp() *regexp.Regexp {
	_fixErrNoSuchTableRegexpOnce.Do(func() {
		_fixErrNoSuchTableRegexp = regexp.MustCompile(`^Table '([^.']+)\.([^.']+)' doesn't exist$`)
	})
	return _fixErrNoSuchTableRegexp
}
