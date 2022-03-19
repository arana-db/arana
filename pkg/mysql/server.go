//
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
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
)

import (
	"github.com/arana-db/parser"
	_ "github.com/arana-db/parser/test_driver"

	err2 "github.com/pkg/errors"

	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/util/log"
)

const initClientConnStatus = mysql.ServerStatusAutocommit

type handshakeResult struct {
	connectionID uint32
	schema       string
	tenant       string
	username     string
	authMethod   string
	authResponse []byte
	salt         []byte
}

type ServerConfig struct {
	ServerVersion string `yaml:"server_version" json:"server_version"`
}

type Listener struct {
	// conf
	conf *ServerConfig

	// This is the main listener socket.
	listener net.Listener

	executor proto.Executor

	// Incrementing ID for connection id.
	connectionID uint32
	// connReadBufferSize is size of buffer for reads from underlying connection.
	// Reads are unbuffered if it's <=0.
	connReadBufferSize int

	// capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	capabilities uint32

	// characterSet is the character set used by the other side of the
	// connection.
	// It is set during the initial handshake.
	// See the values in constants.go.
	characterSet uint8

	// schemaName is the default database name to use. It is set
	// during handshake, and by ComInitDb packets. Both client and
	// servers maintain it. This member is private because it's
	// non-authoritative: the client can change the schema name
	// through the 'USE' statement, which will bypass this variable.
	schemaName string

	// statementID is the prepared statement ID.
	statementID atomic.Uint32

	// stmts is the map to use a prepared statement.
	// key is uint32 value is *proto.Stmt
	stmts sync.Map
}

func NewListener(conf *config.Listener) (proto.Listener, error) {
	cfg := &ServerConfig{
		ServerVersion: conf.ServerVersion,
	}

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.SocketAddress.Address, conf.SocketAddress.Port))
	if err != nil {
		log.Errorf("listen %s:%d error, %s", conf.SocketAddress.Address, conf.SocketAddress.Port, err)
		return nil, err
	}

	listener := &Listener{
		conf:     cfg,
		listener: l,
	}
	return listener, nil
}

func (l *Listener) SetExecutor(executor proto.Executor) {
	l.executor = executor
}

func (l *Listener) Listen() {
	log.Infof("start mysql Listener %s", l.listener.Addr())
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return
		}

		connectionID := l.connectionID
		l.connectionID++

		go l.handle(conn, connectionID)
	}
}

func (l *Listener) Close() {
}

func (l *Listener) handle(conn net.Conn, connectionID uint32) {
	c := newConn(conn)
	c.ConnectionID = connectionID

	// Catch panics, and close the connection in any case.
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("mysql_server caught panic:\n%v", x)
		}

		conn.Close()
		l.executor.ConnectionClose(&proto.Context{
			Context:      context.Background(),
			ConnectionID: l.connectionID,
		})
	}()

	err := l.handshake(c)
	if err != nil {
		werr := c.writeErrorPacketFromError(err)
		if werr != nil {
			log.Errorf("Cannot write error packet to %s: %v", c, werr)
			return
		}
		return
	}

	// Negotiation worked, send OK packet.
	if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
		log.Errorf("Cannot write OK packet to %s: %v", c, err)
		return
	}

	for {
		c.sequence = 0
		data, err := c.readEphemeralPacket()
		if err != nil {
			// Don't log EOF errors. They cause too much spam.
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				log.Errorf("Error reading packet from %s: %v", c, err)
			}
			return
		}

		content := make([]byte, len(data))
		copy(content, data)
		ctx := &proto.Context{
			Context:      context.Background(),
			Schema:       c.Schema,
			ConnectionID: l.connectionID,
			Data:         content,
		}
		err = l.ExecuteCommand(c, ctx)
		if err != nil {
			return
		}
	}
}

func (l *Listener) handshake(c *Conn) error {
	salt, err := newSalt()
	if err != nil {
		return err
	}
	// First build and send the server handshake packet.
	err = l.writeHandshakeV10(c, false, salt)
	if err != nil {
		if err != io.EOF {
			log.Errorf("Cannot send HandshakeV10 packet to %s: %v", c, err)
		}
		return err
	}

	// Wait for the client response. This has to be a direct read,
	// so we don't buffer the TLS negotiation packets.
	response, err := c.readEphemeralPacketDirect()
	if err != nil {
		// Don't log EOF errors. They cause too much spam, same as main read loop.
		if err != io.EOF {
			log.Infof("Cannot read client handshake response from %s: %v, it may not be a valid MySQL client", c, err)
		}
		return err
	}

	c.recycleReadPacket()

	handshake, err := l.parseClientHandshakePacket(true, response)
	if err != nil {
		log.Errorf("Cannot parse client handshake response from %s: %v", c, err)
		return err
	}
	handshake.connectionID = c.ConnectionID
	handshake.salt = salt

	err = l.ValidateHash(handshake)
	if err != nil {
		log.Errorf("Error authenticating user using MySQL native password: %v", err)
		return err
	}

	c.Schema = handshake.schema
	c.Tenant = handshake.tenant

	return nil
}

// writeHandshakeV10 writes the Initial Handshake Packet, server side.
// It returns the salt Content.
func (l *Listener) writeHandshakeV10(c *Conn, enableTLS bool, salt []byte) error {
	capabilities := mysql.CapabilityClientLongPassword |
		mysql.CapabilityClientFoundRows |
		mysql.CapabilityClientLongFlag |
		mysql.CapabilityClientConnectWithDB |
		mysql.CapabilityClientProtocol41 |
		mysql.CapabilityClientTransactions |
		mysql.CapabilityClientSecureConnection |
		mysql.CapabilityClientMultiStatements |
		mysql.CapabilityClientMultiResults |
		mysql.CapabilityClientPluginAuth |
		mysql.CapabilityClientPluginAuthLenencClientData |
		mysql.CapabilityClientDeprecateEOF |
		mysql.CapabilityClientConnAttr
	if enableTLS {
		capabilities |= mysql.CapabilityClientSSL
	}

	length :=
		1 + // protocol version
			lenNullString(l.conf.ServerVersion) +
			4 + // connection ID
			8 + // first part of salt Content
			1 + // filler byte
			2 + // capability flags (lower 2 bytes)
			1 + // character set
			2 + // status flag
			2 + // capability flags (upper 2 bytes)
			1 + // length of auth plugin Content
			10 + // reserved (0)
			13 + // auth-plugin-Content
			lenNullString(mysql.MysqlNativePassword) // auth-plugin-name

	data := c.startEphemeralPacket(length)
	pos := 0

	// Protocol version.
	pos = writeByte(data, pos, mysql.ProtocolVersion)

	// Copy server version.
	pos = writeNullString(data, pos, l.conf.ServerVersion)

	// Add connectionID in.
	pos = writeUint32(data, pos, c.ConnectionID)

	pos += copy(data[pos:], salt[:8])

	// One filler byte, always 0.
	pos = writeByte(data, pos, 0)

	// Lower part of the capability flags.
	pos = writeUint16(data, pos, uint16(capabilities))

	// Character set.
	pos = writeByte(data, pos, mysql.CharacterSetUtf8)

	// Status flag.
	pos = writeUint16(data, pos, initClientConnStatus)

	// Upper part of the capability flags.
	pos = writeUint16(data, pos, uint16(capabilities>>16))

	// Length of auth plugin Content.
	// Always 21 (8 + 13).
	pos = writeByte(data, pos, 21)

	// Reserved 10 bytes: all 0
	pos = writeZeroes(data, pos, 10)

	// Second part of auth plugin Content.
	pos += copy(data[pos:], salt[8:])
	data[pos] = 0
	pos++

	// Copy authPluginName. We always start with mysql_native_password.
	pos = writeNullString(data, pos, mysql.MysqlNativePassword)

	// Sanity check.
	if pos != len(data) {
		return err2.Errorf("error building Handshake packet: got %v bytes expected %v", pos, len(data))
	}

	if err := c.writeEphemeralPacket(); err != nil {
		if strings.HasSuffix(err.Error(), "write: connection reset by peer") {
			return io.EOF
		}
		if strings.HasSuffix(err.Error(), "write: broken pipe") {
			return io.EOF
		}
		return err
	}

	return nil
}

// parseClientHandshakePacket parses the handshake sent by the client.
// Returns the database, username, auth method, auth Content, error.
// The original Content is not pointed at, and can be freed.
func (l *Listener) parseClientHandshakePacket(firstTime bool, data []byte) (*handshakeResult, error) {
	pos := 0

	// Client flags, 4 bytes.
	clientFlags, pos, ok := readUint32(data, pos)
	if !ok {
		return nil, err2.New("parseClientHandshakePacket: can't read client flags")
	}
	if clientFlags&mysql.CapabilityClientProtocol41 == 0 {
		return nil, err2.New("parseClientHandshakePacket: only support protocol 4.1")
	}

	// Remember a subset of the capabilities, so we can use them
	// later in the protocol. If we re-received the handshake packet
	// after SSL negotiation, do not overwrite capabilities.
	if firstTime {
		l.capabilities = clientFlags & (mysql.CapabilityClientDeprecateEOF | mysql.CapabilityClientFoundRows)
	}

	// set connection capability for executing multi statements
	if clientFlags&mysql.CapabilityClientMultiStatements > 0 {
		l.capabilities |= mysql.CapabilityClientMultiStatements
	}

	// Max packet size. Don't do anything with this now.
	// See doc.go for more information.
	_, pos, ok = readUint32(data, pos)
	if !ok {
		return nil, err2.New("parseClientHandshakePacket: can't read maxPacketSize")
	}

	// Character set. Need to handle it.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return nil, err2.New("parseClientHandshakePacket: can't read characterSet")
	}
	l.characterSet = characterSet

	// 23x reserved zero bytes.
	pos += 23

	//// Check for SSL.
	//if firstTime && l.TLSConfig != nil && clientFlags&CapabilityClientSSL > 0 {
	//	// Need to switch to TLS, and then re-read the packet.
	//	conn := tls.Server(c.conn, l.TLSConfig)
	//	c.conn = conn
	//	c.bufferedReader.Reset(conn)
	//	l.capabilities |= CapabilityClientSSL
	//	return "", "", nil, nil
	//}

	// username
	username, pos, ok := readNullString(data, pos)
	if !ok {
		return nil, err2.New("parseClientHandshakePacket: can't read username")
	}

	// auth-response can have three forms.
	var authResponse []byte
	if clientFlags&mysql.CapabilityClientPluginAuthLenencClientData != 0 {
		var l uint64
		l, pos, ok = readLenEncInt(data, pos)
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read auth-response variable length")
		}
		authResponse, pos, ok = readBytesCopy(data, pos, int(l))
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read auth-response")
		}

	} else if clientFlags&mysql.CapabilityClientSecureConnection != 0 {
		var l byte
		l, pos, ok = readByte(data, pos)
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read auth-response length")
		}

		authResponse, pos, ok = readBytesCopy(data, pos, int(l))
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read auth-response")
		}
	} else {
		a := ""
		a, pos, ok = readNullString(data, pos)
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read auth-response")
		}
		authResponse = []byte(a)
	}

	// db name.
	var schemaName string
	if clientFlags&mysql.CapabilityClientConnectWithDB != 0 {
		dbname := ""
		dbname, pos, ok = readNullString(data, pos)
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read dbname")
		}
		schemaName = dbname
	}

	// authMethod (with default)
	authMethod := mysql.MysqlNativePassword
	if clientFlags&mysql.CapabilityClientPluginAuth != 0 {
		authMethod, pos, ok = readNullString(data, pos)
		if !ok {
			return nil, err2.New("parseClientHandshakePacket: can't read authMethod")
		}
	}

	// The JDBC driver sometimes sends an empty string as the auth method when it wants to use mysql_native_password
	if authMethod == "" {
		authMethod = mysql.MysqlNativePassword
	}

	// Decode connection attributes send by the client
	if clientFlags&mysql.CapabilityClientConnAttr != 0 {
		if _, _, err := parseConnAttrs(data, pos); err != nil {
			log.Warnf("Decode connection attributes send by the client: %v", err)
		}
	}

	return &handshakeResult{
		schema:       schemaName,
		username:     username,
		authMethod:   authMethod,
		authResponse: authResponse,
	}, nil
}

func (l *Listener) ValidateHash(handshake *handshakeResult) error {
	tenant, ok := security.DefaultTenantManager().GetTenantOfCluster(handshake.schema)
	if !ok {
		return errors.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", handshake.username)
	}

	user, ok := security.DefaultTenantManager().GetUser(tenant, handshake.username)
	if !ok {
		return errors.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", handshake.username)
	}

	computedAuthResponse := scramblePassword(handshake.salt, user.Password)
	if !bytes.Equal(handshake.authResponse, computedAuthResponse) {
		return errors.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%v'", handshake.username)
	}

	// bind tenant
	handshake.tenant = tenant

	return nil
}

func (l *Listener) ExecuteCommand(c *Conn, ctx *proto.Context) error {
	commandType := ctx.Data[0]
	switch commandType {
	case mysql.ComQuit:
		// https://dev.mysql.com/doc/internals/en/com-quit.html
		c.recycleReadPacket()
		return err2.New("ComQuit")
	case mysql.ComInitDB:
		db := string(ctx.Data[1:])
		c.recycleReadPacket()

		var allow bool
		for _, it := range security.DefaultTenantManager().GetClusters(c.Tenant) {
			if db == it {
				allow = true
				break
			}
		}

		if !allow {
			if err := c.writeErrorPacketFromError(errors.NewSQLError(mysql.ERBadDb, "", "Unknown database '%s'", db)); err != nil {
				log.Errorf("failed to write ComInitDB error to %s: %v", c, err)
				return err
			}
			return nil
		}

		c.Schema = db
		err := l.executor.ExecuteUseDB(ctx)
		if err != nil {
			return err
		}
		if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
			log.Errorf("Error writing ComInitDB result to %s: %v", c, err)
			return err
		}
	case mysql.ComQuery:
		err := func() error {
			c.startWriterBuffering()
			defer func() {
				if err := c.endWriterBuffering(); err != nil {
					log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
				}
			}()

			c.recycleReadPacket()
			result, warn, err := l.executor.ExecutorComQuery(ctx)
			if err != nil {
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					log.Error("Error writing query error to client %v: %v", l.connectionID, werr)
					return werr
				}
				return nil
			}
			rlt := result.(*Result)
			if len(rlt.Fields) == 0 {
				// A successful callback with no fields means that this was a
				// DML or other write-only operation.
				//
				// We should not send any more packets after this, but make sure
				// to extract the affected rows and last insert id from the result
				// struct here since clients expect it.
				return c.writeOKPacket(rlt.AffectedRows, rlt.InsertId, c.StatusFlags, warn)
			}
			err = c.writeFields(l.capabilities, result)
			if err != nil {
				return err
			}
			err = c.writeRows(result)
			if err != nil {
				return err
			}
			if err := c.writeEndResult(l.capabilities, false, 0, 0, warn); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	case mysql.ComPing:
		c.recycleReadPacket()

		// Return error if Listener was shut down and OK otherwise
		if err := c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
			log.Errorf("Error writing ComPing result to %s: %v", c, err)
			return err
		}
	case mysql.ComFieldList:
		c.recycleReadPacket()

		fields, err := l.executor.ExecuteFieldList(ctx)
		if err != nil {
			log.Errorf("Conn %v: Error write field list: %v", c, err)
			if werr := c.writeErrorPacketFromError(err); werr != nil {
				// If we can't even write the error, we're done.
				log.Errorf("Conn %v: Error write field list error: %v", c, werr)
				return werr
			}
		}
		result := &Result{Fields: fields}
		err = c.writeFields(l.capabilities, result)
		if err != nil {
			return err
		}
	case mysql.ComPrepare:
		query := string(ctx.Data[1:])
		c.recycleReadPacket()

		// Popoulate PrepareData
		statementID := l.statementID.Inc()

		stmt := &proto.Stmt{
			StatementID: statementID,
			PrepareStmt: query,
		}
		p := parser.New()
		act, err := p.ParseOneStmt(stmt.PrepareStmt, "", "")
		if err != nil {
			log.Errorf("Conn %v: Error parsing prepared statement: %v", c, err)
			if werr := c.writeErrorPacketFromError(err); werr != nil {
				// If we can't even write the error, we're done.
				log.Errorf("Conn %v: Error writing prepared statement error: %v", c, werr)
				return werr
			}
		}
		stmt.StmtNode = act

		paramsCount := uint16(strings.Count(query, "?"))

		if paramsCount > 0 {
			stmt.ParamsCount = paramsCount
			stmt.ParamsType = make([]int32, paramsCount)
			stmt.BindVars = make(map[string]interface{}, paramsCount)
		}

		l.stmts.Store(statementID, stmt)

		if err := c.writePrepare(l.capabilities, stmt); err != nil {
			return err
		}
	case mysql.ComStmtExecute:
		err := func() error {
			c.startWriterBuffering()
			defer func() {
				if err := c.endWriterBuffering(); err != nil {
					log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
				}
			}()
			stmtID, _, err := c.parseComStmtExecute(&l.stmts, ctx.Data)
			c.recycleReadPacket()

			if stmtID != uint32(0) {
				defer func() {
					// Allocate a new bindvar map every time since VTGate.Execute() mutates it.
					if prepare, ok := l.stmts.Load(stmtID); ok {
						prepareStmt, _ := prepare.(*proto.Stmt)
						prepareStmt.BindVars = make(map[string]interface{}, prepareStmt.ParamsCount)
					}
				}()
			}

			if err != nil {
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					// If we can't even write the error, we're done.
					log.Error("Error writing query error to client %v: %v", l.connectionID, werr)
					return werr
				}
				return nil
			}

			prepareStmt, _ := l.stmts.Load(stmtID)
			ctx.Stmt = prepareStmt.(*proto.Stmt)

			result, warn, err := l.executor.ExecutorComStmtExecute(ctx)
			if err != nil {
				if werr := c.writeErrorPacketFromError(err); werr != nil {
					log.Error("Error writing query error to client %v: %v", l.connectionID, werr)
					return werr
				}
				return nil
			}
			rlt := result.(*Result)
			if len(rlt.Fields) == 0 {
				// A successful callback with no fields means that this was a
				// DML or other write-only operation.
				//
				// We should not send any more packets after this, but make sure
				// to extract the affected rows and last insert id from the result
				// struct here since clients expect it.
				return c.writeOKPacket(rlt.AffectedRows, rlt.InsertId, c.StatusFlags, warn)
			}

			err = c.writeFields(l.capabilities, result)
			if err != nil {
				return err
			}
			err = c.writeBinaryRows(result)
			if err != nil {
				return err
			}
			if err := c.writeEndResult(l.capabilities, false, 0, 0, warn); err != nil {
				log.Errorf("Error writing result to %s: %v", c, err)
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	case mysql.ComStmtClose: // no response
		stmtID, _, ok := readUint32(ctx.Data, 1)
		c.recycleReadPacket()
		if ok {
			l.stmts.Delete(stmtID)
		}
	case mysql.ComStmtSendLongData: // no response
		// todo
	case mysql.ComStmtReset:
		stmtID, _, ok := readUint32(ctx.Data, 1)
		c.recycleReadPacket()
		if ok {
			if prepare, ok := l.stmts.Load(stmtID); ok {
				prepareStmt, _ := prepare.(*proto.Stmt)
				prepareStmt.BindVars = make(map[string]interface{})
			}
		}
		return c.writeOKPacket(0, 0, c.StatusFlags, 0)
	case mysql.ComSetOption:
		operation, _, ok := readUint16(ctx.Data, 1)
		c.recycleReadPacket()
		if ok {
			switch operation {
			case 0:
				l.capabilities |= mysql.CapabilityClientMultiStatements
			case 1:
				l.capabilities &^= mysql.CapabilityClientMultiStatements
			default:
				log.Errorf("Got unhandled packet (ComSetOption default) from client %v, returning error: %v", l.connectionID, ctx.Data)
				if err := c.writeErrorPacket(mysql.ERUnknownComError, mysql.SSUnknownComError, "error handling packet: %v", ctx.Data); err != nil {
					log.Errorf("Error writing error packet to client: %v", err)
					return err
				}
			}
			if err := c.writeEndResult(l.capabilities, false, 0, 0, 0); err != nil {
				log.Errorf("Error writeEndResult error %v ", err)
				return err
			}
		} else {
			log.Errorf("Got unhandled packet (ComSetOption else) from client %v, returning error: %v", l.connectionID, ctx.Data)
			if err := c.writeErrorPacket(mysql.ERUnknownComError, mysql.SSUnknownComError, "error handling packet: %v", ctx.Data); err != nil {
				log.Errorf("Error writing error packet to client: %v", err)
				return err
			}
		}
	}
	return nil
}

func parseConnAttrs(data []byte, pos int) (map[string]string, int, error) {
	var attrLen uint64

	attrLen, pos, ok := readLenEncInt(data, pos)
	if !ok {
		return nil, 0, err2.Errorf("parseClientHandshakePacket: can't read connection attributes variable length")
	}

	var attrLenRead uint64

	attrs := make(map[string]string)

	for attrLenRead < attrLen {
		var keyLen byte
		keyLen, pos, ok = readByte(data, pos)
		if !ok {
			return nil, 0, err2.Errorf("parseClientHandshakePacket: can't read connection attribute key length")
		}
		attrLenRead += uint64(keyLen) + 1

		var connAttrKey []byte
		connAttrKey, pos, ok = readBytesCopy(data, pos, int(keyLen))
		if !ok {
			return nil, 0, err2.Errorf("parseClientHandshakePacket: can't read connection attribute key")
		}

		var valLen byte
		valLen, pos, ok = readByte(data, pos)
		if !ok {
			return nil, 0, err2.Errorf("parseClientHandshakePacket: can't read connection attribute value length")
		}
		attrLenRead += uint64(valLen) + 1

		var connAttrVal []byte
		connAttrVal, pos, ok = readBytesCopy(data, pos, int(valLen))
		if !ok {
			return nil, 0, err2.Errorf("parseClientHandshakePacket: can't read connection attribute value")
		}

		attrs[string(connAttrKey[:])] = string(connAttrVal[:])
	}

	return attrs, pos, nil
}

// newSalt returns a 20 character salt.
func newSalt() ([]byte, error) {
	salt := make([]byte, 20)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	// Salt must be a legal UTF8 string.
	for i := 0; i < len(salt); i++ {
		salt[i] &= 0x7f
		if salt[i] == '\x00' || salt[i] == '$' {
			salt[i]++
		}
	}

	return salt, nil
}

func (c *Conn) sendColumnCount(count uint64) error {
	length := lenEncIntSize(count)
	data := c.startEphemeralPacket(length)
	writeLenEncInt(data, 0, count)
	return c.writeEphemeralPacket()
}

func (c *Conn) parseComStmtExecute(stmts *sync.Map, data []byte) (uint32, byte, error) {
	pos := 0
	payload := data[1:]
	bitMap := make([]byte, 0)

	// statement ID
	stmtID, pos, ok := readUint32(payload, 0)
	if !ok {
		return 0, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "reading statement ID failed")
	}
	//prepare, ok := stmts[stmtID]
	prepare, ok := stmts.Load(stmtID)
	if !ok {
		return 0, 0, errors.NewSQLError(mysql.CRCommandsOutOfSync, mysql.SSUnknownSQLState, "statement ID is not found from record")
	}
	prepareStmt, _ := prepare.(*proto.Stmt)
	// cursor type flags
	cursorType, pos, ok := readByte(payload, pos)
	if !ok {
		return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "reading cursor type flags failed")
	}

	// iteration count
	iterCount, pos, ok := readUint32(payload, pos)
	if !ok {
		return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "reading iteration count failed")
	}
	if iterCount != uint32(1) {
		return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "iteration count is not equal to 1")
	}

	if prepareStmt.ParamsCount > 0 {
		bitMap, pos, ok = readBytes(payload, pos, int((prepareStmt.ParamsCount+7)/8))
		if !ok {
			return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "reading NULL-bitmap failed")
		}
	}

	newParamsBoundFlag, pos, ok := readByte(payload, pos)
	if ok && newParamsBoundFlag == 0x01 {
		var mysqlType, flags byte
		for i := uint16(0); i < prepareStmt.ParamsCount; i++ {
			mysqlType, pos, ok = readByte(payload, pos)
			if !ok {
				return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "reading parameter type failed")
			}

			flags, pos, ok = readByte(payload, pos)
			if !ok {
				return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "reading parameter flags failed")
			}

			// convert MySQL type to internal type.
			valType, err := mysql.MySQLToType(int64(mysqlType), int64(flags))
			if err != nil {
				return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "MySQLToType(%v,%v) failed: %v", mysqlType, flags, err)
			}

			prepareStmt.ParamsType[i] = int32(valType)
		}
	}

	for i := 0; i < len(prepareStmt.ParamsType); i++ {
		var val interface{}
		parameterID := fmt.Sprintf("v%d", i+1)
		if v, ok := prepareStmt.BindVars[parameterID]; ok {
			if v != nil {
				continue
			}
		}

		if (bitMap[i/8] & (1 << uint(i%8))) > 0 {
			val, pos, ok = c.parseStmtArgs(nil, mysql.FieldTypeNULL, pos)
		} else {
			val, pos, ok = c.parseStmtArgs(payload, mysql.FieldType(prepareStmt.ParamsType[i]), pos)
		}
		if !ok {
			return stmtID, 0, errors.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "decoding parameter value failed: %v", prepareStmt.ParamsType[i])
		}

		prepareStmt.BindVars[parameterID] = val
	}

	return stmtID, cursorType, nil
}

func (c *Conn) parseStmtArgs(data []byte, typ mysql.FieldType, pos int) (interface{}, int, bool) {
	switch typ {
	case mysql.FieldTypeNULL:
		return nil, pos, true
	case mysql.FieldTypeTiny:
		val, pos, ok := readByte(data, pos)
		return int64(int8(val)), pos, ok
	case mysql.FieldTypeUint8:
		val, pos, ok := readByte(data, pos)
		return int64(int8(val)), pos, ok
	case mysql.FieldTypeUint16:
		val, pos, ok := readUint16(data, pos)
		return int64(int16(val)), pos, ok
	case mysql.FieldTypeShort, mysql.FieldTypeYear:
		val, pos, ok := readUint16(data, pos)
		return int64(int16(val)), pos, ok
	case mysql.FieldTypeUint24, mysql.FieldTypeUint32:
		val, pos, ok := readUint32(data, pos)
		return int64(val), pos, ok
	case mysql.FieldTypeInt24, mysql.FieldTypeLong:
		val, pos, ok := readUint32(data, pos)
		return int64(int32(val)), pos, ok
	case mysql.FieldTypeFloat:
		val, pos, ok := readUint32(data, pos)
		return math.Float32frombits(uint32(val)), pos, ok
	case mysql.FieldTypeUint64:
		val, pos, ok := readUint64(data, pos)
		return val, pos, ok
	case mysql.FieldTypeLongLong:
		val, pos, ok := readUint64(data, pos)
		return int64(val), pos, ok
	case mysql.FieldTypeDouble:
		val, pos, ok := readUint64(data, pos)
		return math.Float64frombits(val), pos, ok
	case mysql.FieldTypeTimestamp, mysql.FieldTypeDate, mysql.FieldTypeDateTime:
		size, pos, ok := readByte(data, pos)
		if !ok {
			return nil, 0, false
		}
		switch size {
		case 0x00:
			return []byte{' '}, pos, ok
		case 0x0b:
			year, pos, ok := readUint16(data, pos)
			if !ok {
				return nil, 0, false
			}
			month, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			day, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			minute, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			microSecond, pos, ok := readUint32(data, pos)
			if !ok {
				return nil, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				fmt.Sprintf("%06d", microSecond)

			return []byte(val), pos, ok
		case 0x07:
			year, pos, ok := readUint16(data, pos)
			if !ok {
				return nil, 0, false
			}
			month, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			day, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			minute, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))

			return []byte(val), pos, ok
		case 0x04:
			year, pos, ok := readUint16(data, pos)
			if !ok {
				return nil, 0, false
			}
			month, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			day, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day))

			return []byte(val), pos, ok
		default:
			return nil, 0, false
		}
	case mysql.FieldTypeTime:
		size, pos, ok := readByte(data, pos)
		if !ok {
			return nil, 0, false
		}
		switch size {
		case 0x00:
			return []byte("00:00:00"), pos, ok
		case 0x0c:
			isNegative, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			days, pos, ok := readUint32(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}

			hours := uint32(hour) + days*uint32(24)

			minute, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			microSecond, pos, ok := readUint32(data, pos)
			if !ok {
				return nil, 0, false
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				fmt.Sprintf("%06d", microSecond)

			return []byte(val), pos, ok
		case 0x08:
			isNegative, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			days, pos, ok := readUint32(data, pos)
			if !ok {
				return nil, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}

			hours := uint32(hour) + days*uint32(24)

			minute, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return nil, 0, false
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))

			return []byte(val), pos, ok
		default:
			return nil, 0, false
		}
	case mysql.FieldTypeDecimal, mysql.FieldTypeNewDecimal, mysql.FieldTypeVarChar, mysql.FieldTypeTinyBLOB,
		mysql.FieldTypeMediumBLOB, mysql.FieldTypeLongBLOB, mysql.FieldTypeBLOB, mysql.FieldTypeVarString,
		mysql.FieldTypeString, mysql.FieldTypeGeometry, mysql.FieldTypeJSON, mysql.FieldTypeBit,
		mysql.FieldTypeEnum, mysql.FieldTypeSet:
		val, pos, ok := readLenEncStringAsBytesCopy(data, pos)
		return val, pos, ok
	default:
		return nil, pos, false
	}
}

func (c *Conn) writeColumnDefinition(field *Field) error {
	length := 4 + // lenEncStringSize("def")
		lenEncStringSize(field.database) +
		lenEncStringSize(field.table) +
		lenEncStringSize(field.orgTable) +
		lenEncStringSize(field.name) +
		lenEncStringSize(field.orgName) +
		1 + // length of fixed length fields
		2 + // character set
		4 + // column length
		1 + // type
		2 + // flags
		1 + // decimals
		2 // filler

	// Get the type and the flags back. If the Field contains
	// non-zero flags, we use them. Otherwise use the flags we
	// derive from the type.
	typ, flags := mysql.TypeToMySQL(field.fieldType)
	if field.flags != 0 {
		flags = int64(field.flags)
	}

	data := c.startEphemeralPacket(length)
	pos := 0

	pos = writeLenEncString(data, pos, "def") // Always the same.
	pos = writeLenEncString(data, pos, field.database)
	pos = writeLenEncString(data, pos, field.table)
	pos = writeLenEncString(data, pos, field.orgTable)
	pos = writeLenEncString(data, pos, field.name)
	pos = writeLenEncString(data, pos, field.orgName)
	pos = writeByte(data, pos, 0x0c)
	pos = writeUint16(data, pos, field.charSet)
	pos = writeUint32(data, pos, field.columnLength)
	pos = writeByte(data, pos, byte(typ))
	pos = writeUint16(data, pos, uint16(flags))
	pos = writeByte(data, pos, byte(field.decimals))
	pos = writeUint16(data, pos, uint16(0x0000))

	if pos != len(data) {
		return fmt.Errorf("packing of column definition used %v bytes instead of %v", pos, len(data))
	}

	return c.writeEphemeralPacket()
}

// writeFields writes the fields of a Result. It should be called only
// if there are valid Columns in the result.
func (c *Conn) writeFields(capabilities uint32, result proto.Result) error {
	// Send the number of fields first.
	rlt := result.(*Result)
	if err := c.sendColumnCount(uint64(len(rlt.Fields))); err != nil {
		return err
	}

	// Now send each Field.
	for _, field := range rlt.Fields {
		fld := field.(*Field)
		if err := c.writeColumnDefinition(fld); err != nil {
			return err
		}
	}

	// Now send an EOF packet.
	if capabilities&mysql.CapabilityClientDeprecateEOF == 0 {
		// With CapabilityClientDeprecateEOF, we do not send this EOF.
		if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) writeRow(row []*proto.Value) error {
	length := 0
	for _, val := range row {
		if val == nil || val.Val == nil {
			length++
		} else {
			l := len(val.Raw)
			length += lenEncIntSize(uint64(l)) + l
		}
	}

	data := c.startEphemeralPacket(length)
	pos := 0
	for _, val := range row {
		if val == nil || val.Val == nil {
			pos = writeByte(data, pos, mysql.NullValue)
		} else {
			l := len(val.Raw)
			pos = writeLenEncInt(data, pos, uint64(l))
			pos += copy(data[pos:], val.Raw)
		}
	}

	if pos != length {
		return err2.Errorf("packet row: got %v bytes but expected %v", pos, length)
	}

	return c.writeEphemeralPacket()
}

// writeRows sends the rows of a Result.
func (c *Conn) writeRows(result proto.Result) error {
	rlt := result.(*Result)
	for _, row := range rlt.Rows {
		r := row.(*Row)
		textRow := TextRow{*r}
		values, err := textRow.Decode()
		if err != nil {
			return err
		}
		if err := c.writeRow(values); err != nil {
			return err
		}
	}
	return nil
}

// writeEndResult concludes the sending of a Result.
// if more is set to true, then it means there are more results afterwords
func (c *Conn) writeEndResult(capabilities uint32, more bool, affectedRows, lastInsertID uint64, warnings uint16) error {
	// Send either an EOF, or an OK packet.
	// See doc.go.
	flags := c.StatusFlags
	if more {
		flags |= mysql.ServerMoreResultsExists
	}
	if capabilities&mysql.CapabilityClientDeprecateEOF == 0 {
		if err := c.writeEOFPacket(flags, warnings); err != nil {
			return err
		}
	} else {
		// This will flush too.
		if err := c.writeOKPacketWithEOFHeader(affectedRows, lastInsertID, flags, warnings); err != nil {
			return err
		}
	}

	return nil
}

// writePrepare writes a prepare query response to the wire.
func (c *Conn) writePrepare(capabilities uint32, prepare *proto.Stmt) error {
	paramsCount := prepare.ParamsCount

	data := c.startEphemeralPacket(12)
	pos := 0

	pos = writeByte(data, pos, 0x00)
	pos = writeUint32(data, pos, prepare.StatementID)
	pos = writeUint16(data, pos, uint16(0))
	pos = writeUint16(data, pos, paramsCount)
	pos = writeByte(data, pos, 0x00)
	writeUint16(data, pos, 0x0000)

	if err := c.writeEphemeralPacket(); err != nil {
		return err
	}

	if paramsCount > 0 {
		for i := uint16(0); i < paramsCount; i++ {
			if err := c.writeColumnDefinition(&Field{
				name:      "?",
				fieldType: mysql.FieldTypeString,
				flags:     mysql.BinaryFlag,
				charSet:   63,
			}); err != nil {
				return err
			}
		}

		// Now send an EOF packet.
		if capabilities&mysql.CapabilityClientDeprecateEOF == 0 {
			// With CapabilityClientDeprecateEOF, we do not send this EOF.
			if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Conn) writeBinaryRow(fields []proto.Field, row []*proto.Value) error {
	length := 0
	nullBitMapLen := (len(fields) + 7 + 2) / 8
	for _, val := range row {
		if val != nil && val.Val != nil {
			l, err := val2MySQLLen(val)
			if err != nil {
				return fmt.Errorf("internal value %v get MySQL value length error: %v", val, err)
			}
			length += l
		}
	}

	length += nullBitMapLen + 1

	Data := c.startEphemeralPacket(length)
	pos := 0

	pos = writeByte(Data, pos, 0x00)

	for i := 0; i < nullBitMapLen; i++ {
		pos = writeByte(Data, pos, 0x00)
	}

	for i, val := range row {
		if val == nil || val.Val == nil {
			bytePos := (i+2)/8 + 1
			bitPos := (i + 2) % 8
			Data[bytePos] |= 1 << uint(bitPos)
		} else {
			v, err := val2MySQL(val)
			if err != nil {
				c.recycleWritePacket()
				return fmt.Errorf("internal value %v to MySQL value error: %v", val, err)
			}
			pos += copy(Data[pos:], v)
		}
	}

	if pos != length {
		return fmt.Errorf("internal error packet row: got %v bytes but expected %v", pos, length)
	}

	return c.writeEphemeralPacket()
}

// writeTextToBinaryRows sends the rows of a Result with binary form.
func (c *Conn) writeTextToBinaryRows(result proto.Result) error {
	rlt := result.(*Result)
	for _, row := range rlt.Rows {
		r := row.(*Row)
		textRow := TextRow{*r}
		values, err := textRow.Decode()
		if err != nil {
			return err
		}
		if err := c.writeBinaryRow(rlt.Fields, values); err != nil {
			return err
		}
	}
	return nil
}

func val2MySQL(v *proto.Value) ([]byte, error) {
	var out []byte
	pos := 0
	if v == nil {
		return out, nil
	}

	switch v.Typ {
	case mysql.FieldTypeNULL:
		// no-op
	case mysql.FieldTypeTiny:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case mysql.FieldTypeUint8:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case mysql.FieldTypeUint16:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case mysql.FieldTypeShort, mysql.FieldTypeYear:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case mysql.FieldTypeUint24, mysql.FieldTypeUint32:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case mysql.FieldTypeInt24, mysql.FieldTypeLong:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case mysql.FieldTypeFloat:
		val, err := strconv.ParseFloat(fmt.Sprintf("%s", v.Val), 32)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float32bits(float32(val))
		out = make([]byte, 4)
		writeUint32(out, pos, bits)
	case mysql.FieldTypeUint64:
		val, err := strconv.ParseUint(fmt.Sprintf("%s", v.Val), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case mysql.FieldTypeLongLong:
		val, err := strconv.ParseInt(fmt.Sprintf("%s", v.Val), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case mysql.FieldTypeDouble:
		val, err := strconv.ParseFloat(fmt.Sprintf("%s", v.Val), 64)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float64bits(val)
		out = make([]byte, 8)
		writeUint64(out, pos, bits)
	case mysql.FieldTypeTimestamp, mysql.FieldTypeDate, mysql.FieldTypeDateTime:
		if len(v.Raw) > 19 {
			out = make([]byte, 1+11)
			out[pos] = 0x0b
			pos++
			year, err := strconv.ParseUint(string(v.Raw[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.Raw[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.Raw[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.Raw[17:19]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			val := make([]byte, 6)
			count := copy(val, v.Raw[20:])
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSecond, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			pos = writeByte(out, pos, byte(second))
			writeUint32(out, pos, uint32(microSecond))
		} else if len(v.Raw) > 10 {
			out = make([]byte, 1+7)
			out[pos] = 0x07
			pos++
			year, err := strconv.ParseUint(string(v.Raw[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.Raw[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.Raw[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.Raw[17:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			writeByte(out, pos, byte(second))
		} else if len(v.Raw) > 0 {
			out = make([]byte, 1+4)
			out[pos] = 0x04
			pos++
			year, err := strconv.ParseUint(string(v.Raw[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw[8:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			writeByte(out, pos, byte(day))
		} else {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
	case mysql.FieldTypeTime:
		if string(v.Raw) == "00:00:00" {
			out = make([]byte, 1)
			out[pos] = 0x00
		} else if strings.Contains(string(v.Raw), ".") {
			out = make([]byte, 1+12)
			out[pos] = 0x0c
			pos++

			sub1 := strings.Split(string(v.Raw), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}
			sub2 := strings.Split(sub1[2], ".")
			if len(sub2) != 2 {
				err := fmt.Errorf("incorrect time value, '.' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub2[0]
			microSecond := sub2[1]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			pos = writeByte(out, pos, byte(seconds))

			val := make([]byte, 6)
			count := copy(val, microSecond)
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSeconds, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			writeUint32(out, pos, uint32(microSeconds))
		} else if len(v.Raw) > 0 {
			out = make([]byte, 1+8)
			out[pos] = 0x08
			pos++

			sub1 := strings.Split(string(v.Raw), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub1[2]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			writeByte(out, pos, byte(seconds))
		} else {
			err := fmt.Errorf("incorrect time value")
			return []byte{}, err
		}
	case mysql.FieldTypeDecimal, mysql.FieldTypeNewDecimal, mysql.FieldTypeVarChar, mysql.FieldTypeTinyBLOB,
		mysql.FieldTypeMediumBLOB, mysql.FieldTypeLongBLOB, mysql.FieldTypeBLOB, mysql.FieldTypeVarString,
		mysql.FieldTypeString, mysql.FieldTypeGeometry, mysql.FieldTypeJSON, mysql.FieldTypeBit,
		mysql.FieldTypeEnum, mysql.FieldTypeSet:
		l := len(v.Raw)
		length := lenEncIntSize(uint64(l)) + l
		out = make([]byte, length)
		pos = writeLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.Raw)
	default:
		out = make([]byte, len(v.Raw))
		copy(out, v.Raw)
	}
	return out, nil
}

func val2MySQLLen(v *proto.Value) (int, error) {
	var length int
	var err error
	if v == nil {
		return 0, nil
	}

	switch v.Typ {
	case mysql.FieldTypeNULL:
		length = 0
	case mysql.FieldTypeTiny, mysql.FieldTypeUint8:
		length = 1
	case mysql.FieldTypeUint16, mysql.FieldTypeShort, mysql.FieldTypeYear:
		length = 2
	case mysql.FieldTypeUint24, mysql.FieldTypeUint32, mysql.FieldTypeInt24, mysql.FieldTypeLong, mysql.FieldTypeFloat:
		length = 4
	case mysql.FieldTypeUint64, mysql.FieldTypeLongLong, mysql.FieldTypeDouble:
		length = 8
	case mysql.FieldTypeTimestamp, mysql.FieldTypeDate, mysql.FieldTypeDateTime:
		if len(v.Raw) > 19 {
			length = 12
		} else if len(v.Raw) > 10 {
			length = 8
		} else if len(v.Raw) > 0 {
			length = 5
		} else {
			length = 1
		}
	case mysql.FieldTypeTime:
		if string(v.Raw) == "00:00:00" {
			length = 1
		} else if strings.Contains(string(v.Raw), ".") {
			length = 13
		} else if len(v.Raw) > 0 {
			length = 9
		} else {
			err = fmt.Errorf("incorrect time value")
		}
	case mysql.FieldTypeDecimal, mysql.FieldTypeNewDecimal, mysql.FieldTypeVarChar, mysql.FieldTypeTinyBLOB,
		mysql.FieldTypeMediumBLOB, mysql.FieldTypeLongBLOB, mysql.FieldTypeBLOB, mysql.FieldTypeVarString,
		mysql.FieldTypeString, mysql.FieldTypeGeometry, mysql.FieldTypeJSON, mysql.FieldTypeBit,
		mysql.FieldTypeEnum, mysql.FieldTypeSet:
		l := len(v.Raw)
		length = lenEncIntSize(uint64(l)) + l
	default:
		length = len(v.Raw)
	}
	if err != nil {
		return 0, err
	}
	return length, nil
}

func (c *Conn) writeBinaryRows(result proto.Result) error {
	rlt := result.(*Result)
	for _, row := range rlt.Rows {
		r := row.(*Row)
		if err := c.writePacket(r.Data()); err != nil {
			return err
		}
	}
	return nil
}
