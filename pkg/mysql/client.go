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
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	err2 "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/third_party/pools"
)

type Config struct {
	User             string            // Username
	Passwd           string            // Password (requires User)
	Net              string            // Network type
	Addr             string            // Network address (requires Net)
	DBName           string            // Database name
	Params           map[string]string // Connection parameters
	Collation        string            // Connection collation
	Loc              *time.Location    // Location for time.Time values
	MaxAllowedPacket int               // Max packet size allowed
	ServerPubKey     string            // Server public key name
	pubKey           *rsa.PublicKey    // Server public key
	TLSConfig        string            // TLS configuration name
	tls              *tls.Config       // TLS configuration
	Timeout          time.Duration     // Dial timeout
	ReadTimeout      time.Duration     // I/O read timeout
	WriteTimeout     time.Duration     // I/O write timeout

	AllowAllFiles             bool // Allow all files to be used with LOAD DATA LOCAL INFILE
	AllowCleartextPasswords   bool // Allows the cleartext client side plugin
	AllowNativePasswords      bool // Allows the native password authentication method
	AllowOldPasswords         bool // Allows the old insecure password method
	CheckConnLiveness         bool // Check connections for liveness before using them
	ClientFoundRows           bool // Return number of matching rows instead of rows changed
	ColumnsWithAlias          bool // Prepend table alias to column names
	InterpolateParams         bool // Interpolate placeholders into query string
	MultiStatements           bool // Allow multiple statements in one query
	ParseTime                 bool // Parse time values to time.Time
	RejectReadOnly            bool // Reject read-only connections
	DisableClientDeprecateEOF bool // Disable client deprecate EOF
}

// NewConfig creates a new ServerConfig and sets default values.
func NewConfig() *Config {
	return &Config{
		Collation:                 mysql.DefaultCollation,
		Loc:                       time.UTC,
		MaxAllowedPacket:          mysql.DefaultMaxAllowedPacket,
		AllowNativePasswords:      true,
		CheckConnLiveness:         true,
		DisableClientDeprecateEOF: true,
	}
}

func (cfg *Config) Clone() *Config {
	cp := *cfg
	if cp.tls != nil {
		cp.tls = cfg.tls.Clone()
	}
	if len(cp.Params) > 0 {
		cp.Params = make(map[string]string, len(cfg.Params))
		for k, v := range cfg.Params {
			cp.Params[k] = v
		}
	}
	if cfg.pubKey != nil {
		cp.pubKey = &rsa.PublicKey{
			N: new(big.Int).Set(cfg.pubKey.N),
			E: cfg.pubKey.E,
		}
	}
	return &cp
}

func (cfg *Config) normalize() error {
	if cfg.InterpolateParams && mysql.UnsafeCollations[cfg.Collation] {
		return errors.New("interpolateParams can not be used with unsafe collations")
	}

	// Set default network if empty
	if cfg.Net == "" {
		cfg.Net = "tcp"
	}

	// Set default address if empty
	if cfg.Addr == "" {
		switch cfg.Net {
		case "tcp":
			cfg.Addr = "127.0.0.1:3306"
		case "unix":
			cfg.Addr = "/tmp/mysql.sock"
		default:
			return errors.New("default addr for network '" + cfg.Net + "' unknown")
		}
	} else if cfg.Net == "tcp" {
		cfg.Addr = ensureHavePort(cfg.Addr)
	}

	switch cfg.TLSConfig {
	case "false", "":
		// don't set anything
	case "true":
		cfg.tls = &tls.Config{}
	case "skip-verify", "preferred":
		cfg.tls = &tls.Config{InsecureSkipVerify: true}
	default:
		cfg.tls = getTLSConfigClone(cfg.TLSConfig)
		if cfg.tls == nil {
			return errors.New("invalid value / unknown config name: " + cfg.TLSConfig)
		}
	}

	if cfg.tls != nil && cfg.tls.ServerName == "" && !cfg.tls.InsecureSkipVerify {
		host, _, err := net.SplitHostPort(cfg.Addr)
		if err == nil {
			cfg.tls.ServerName = host
		}
	}

	if cfg.ServerPubKey != "" {
		cfg.pubKey = getServerPubKey(cfg.ServerPubKey)
		if cfg.pubKey == nil {
			return errors.New("invalid value / unknown server pub key name: " + cfg.ServerPubKey)
		}
	}

	return nil
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = NewConfig()

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Passwd = dsn[k+1 : j]
								break
							}
						}
						cfg.User = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, err2.ErrInvalidDSNUnescaped
							}
							return nil, err2.ErrInvalidDSNAddr
						}
						cfg.Addr = dsn[k+1 : i-1]
						break
					}
				}
				cfg.Net = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			cfg.DBName = dsn[i+1 : j]

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, err2.ErrInvalidDSNNoSlash
	}

	if err = cfg.normalize(); err != nil {
		return nil, err
	}
	return
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		// cfg params
		switch value := param[1]; param[0] {
		// Disable INFILE allowlist / enable all files
		case "allowAllFiles":
			var isBool bool
			cfg.AllowAllFiles, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Use cleartext authentication mode (MySQL 5.5.10+)
		case "allowCleartextPasswords":
			var isBool bool
			cfg.AllowCleartextPasswords, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Use native password authentication
		case "allowNativePasswords":
			var isBool bool
			cfg.AllowNativePasswords, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Use old authentication mode (pre MySQL 4.1)
		case "allowOldPasswords":
			var isBool bool
			cfg.AllowOldPasswords, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Check connections for Liveness before using them
		case "checkConnLiveness":
			var isBool bool
			cfg.CheckConnLiveness, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Switch "rowsAffected" mode
		case "clientFoundRows":
			var isBool bool
			cfg.ClientFoundRows, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Collation
		case "collation":
			cfg.Collation = value
			break

		case "columnsWithAlias":
			var isBool bool
			cfg.ColumnsWithAlias, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Compression
		case "compress":
			return errors.New("compression not implemented yet")

		// Enable client side placeholder substitution
		case "interpolateParams":
			var isBool bool
			cfg.InterpolateParams, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Time Location
		case "loc":
			if value, err = url.QueryUnescape(value); err != nil {
				return
			}
			cfg.Loc, err = time.LoadLocation(value)
			if err != nil {
				return
			}

		// multiple statements in one query
		case "multiStatements":
			var isBool bool
			cfg.MultiStatements, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// time.Time parsing
		case "parseTime":
			var isBool bool
			cfg.ParseTime, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// I/O read Timeout
		case "readTimeout":
			cfg.ReadTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// Reject read-only connections
		case "rejectReadOnly":
			var isBool bool
			cfg.RejectReadOnly, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Disable client deprecate EOF
		case "disableClientDeprecateEOF":
			var isBool bool
			cfg.DisableClientDeprecateEOF, isBool = readBool(value)
			if !isBool {
				return errors.New("invalid bool value: " + value)
			}

		// Server public key
		case "serverPubKey":
			name, err := url.QueryUnescape(value)
			if err != nil {
				return fmt.Errorf("invalid value for server pub key name: %v", err)
			}
			cfg.ServerPubKey = name

		// Strict mode
		case "strict":
			panic("strict mode has been removed. See https://github.com/go-sql-driver/mysql/wiki/strict-mode")

		// Dial Timeout
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}

		// TLS-Encryption
		case "tls":
			boolValue, isBool := readBool(value)
			if isBool {
				if boolValue {
					cfg.TLSConfig = "true"
				} else {
					cfg.TLSConfig = "false"
				}
			} else if vl := strings.ToLower(value); vl == "skip-verify" || vl == "preferred" {
				cfg.TLSConfig = vl
			} else {
				name, err := url.QueryUnescape(value)
				if err != nil {
					return fmt.Errorf("invalid value for TLS config name: %v", err)
				}
				cfg.TLSConfig = name
			}

		// I/O write Timeout
		case "writeTimeout":
			cfg.WriteTimeout, err = time.ParseDuration(value)
			if err != nil {
				return
			}
		case "maxAllowedPacket":
			cfg.MaxAllowedPacket, err = strconv.Atoi(value)
			if err != nil {
				return
			}
		default:
			// lazy init
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}

			if cfg.Params[param[0]], err = url.QueryUnescape(value); err != nil {
				return
			}
		}
	}

	return
}

func ensureHavePort(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return net.JoinHostPort(addr, "3306")
	}
	return addr
}

type Connector struct {
	conf *Config
}

func NewConnector(config json.RawMessage) (*Connector, error) {
	v := &struct {
		DSN string `json:"dsn"`
	}{}
	if err := json.Unmarshal(config, v); err != nil {
		log.Errorf("unmarshal mysql Listener config failed, %s", err)
		return nil, err
	}
	cfg, err := ParseDSN(v.DSN)
	if err != nil {
		return nil, err
	}
	return &Connector{cfg}, nil
}

func (c *Connector) NewBackendConnection(ctx context.Context) (pools.Resource, error) {
	conn := &BackendConnection{conf: c.conf}
	err := conn.Connect(ctx)
	return conn, err
}

type BackendConnection struct {
	c *Conn

	conf *Config

	// capabilities is the current set of features this connection
	// is using.  It is the features that are both supported by
	// the client and the server, and currently in use.
	// It is set during the initial handshake.
	//
	// It is only used for CapabilityClientDeprecateEOF
	// and CapabilityClientFoundRows.
	capabilities uint32

	serverVersion string

	characterSet uint8
}

func (conn *BackendConnection) DBName() string {
	return conn.conf.DBName
}

func (conn *BackendConnection) Connect(ctx context.Context) error {
	if conn.c != nil {
		conn.c.Close()
	}

	typ := "tcp"
	if conn.conf.Net == "" {
		if strings.Contains(conn.conf.Addr, "/") {
			typ = "unix"
		}
	} else {
		typ = conn.conf.Net
	}
	netConn, err := net.Dial(typ, conn.conf.Addr)
	if err != nil {
		return err
	}
	tcpConn := netConn.(*net.TCPConn)
	// SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that Content is sent as soon as possible after a Write.
	if err := tcpConn.SetNoDelay(true); err != nil {
		return err
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}

	conn.c = newConn(tcpConn)

	return conn.clientHandshake()
}

func (conn *BackendConnection) clientHandshake() error {
	// Wait for the server initial handshake packet, and parse it.
	data, err := conn.c.readPacket()
	if err != nil {
		return err2.NewSQLError(mysql.CRServerLost, "", "initial packet read failed: %v", err)
	}
	capabilities, salt, plugin, err := conn.parseInitialHandshakePacket(data)
	if err != nil {
		return err
	}

	conn.capabilities = 0
	if !conn.conf.DisableClientDeprecateEOF {
		conn.capabilities = capabilities & (mysql.CapabilityClientDeprecateEOF)
	}

	//// Password encryption.
	//scrambledPassword := ScramblePassword(salt, []byte(conn.Passwd))
	authResp, err := conn.auth(salt, plugin)
	if err != nil {
		return err
	}

	// Build and send our handshake response 41.
	// Note this one will never have SSL flag on.
	if err := conn.writeHandshakeResponse41(capabilities, authResp, plugin); err != nil {
		return err
	}

	// Handle response to auth packet, switch methods if possible
	if err = conn.handleAuthResult(salt, plugin); err != nil {
		// Authentication failed and MySQL has already closed the connection
		// (https://dev.mysql.com/doc/internals/en/authentication-fails.html).
		// Do not send COM_QUIT, just cleanup and return the error.
		conn.c.Close()
		return err
	}

	// If the server didn't support DbName in its handshake, set
	// it now. This is what the 'mysql' client does.
	if capabilities&mysql.CapabilityClientConnectWithDB == 0 && conn.conf.DBName != "" {
		// Write the packet.
		if err := conn.WriteComInitDB(conn.conf.DBName); err != nil {
			return err
		}

		// Wait for response, should be OK.
		response, err := conn.c.readPacket()
		conn.c.recycleReadPacket()
		if err != nil {
			return err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", err)
		}
		switch response[0] {
		case mysql.OKPacket:
			// OK packet, we are authenticated.
			return nil
		case mysql.ErrPacket:
			return ParseErrorPacket(response)
		default:
			// FIXME(alainjobart) handle extra auth cases and so on.
			return err2.NewSQLError(mysql.CRServerHandshakeErr, mysql.SSUnknownSQLState, "initial server response is asking for more information, not implemented yet: %v", response)
		}
	}

	return nil
}

// parseInitialHandshakePacket parses the initial handshake from the server.
// It returns a SQLError with the right code.
func (conn *BackendConnection) parseInitialHandshakePacket(data []byte) (uint32, []byte, string, error) {
	pos := 0

	// Protocol version.
	pver, pos, ok := readByte(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRVersionError, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no protocol version")
	}

	// Server is allowed to immediately send ERR packet
	if pver == mysql.ErrPacket {
		errorCode, pos, _ := readUint16(data, pos)
		// Normally there would be a 1-byte sql_state_marker field and a 5-byte
		// sql_state field here, but docs say these will not be present in this case.
		errorMsg, _, _ := readEOFString(data, pos)
		return 0, nil, "", err2.NewSQLError(mysql.CRServerHandshakeErr, mysql.SSUnknownSQLState, "immediate error from server errorCode=%v errorMsg=%v", errorCode, errorMsg)
	}

	if pver != mysql.ProtocolVersion {
		return 0, nil, "", err2.NewSQLError(mysql.CRVersionError, mysql.SSUnknownSQLState, "bad protocol version: %v", pver)
	}

	// Read the server version.
	conn.serverVersion, pos, ok = readNullString(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no server version")
	}

	// Read the connection id.
	conn.c.ConnectionID, pos, ok = readUint32(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no connection id")
	}

	// Read the first part of the auth-plugin-Content
	authPluginData, pos, ok := readBytes(data, pos, 8)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-Content-part-1")
	}

	// One byte filler, 0. We don't really care about the value.
	_, pos, ok = readByte(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no filler")
	}

	// Lower 2 bytes of the capability flags.
	capLower, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (lower 2 bytes)")
	}
	var capabilities = uint32(capLower)

	// The packet can end here.
	if pos == len(data) {
		return capabilities, authPluginData, "", nil
	}

	// Character set.
	characterSet, pos, ok := readByte(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no character set")
	}
	conn.characterSet = characterSet

	// Status flags. Ignored.
	_, pos, ok = readUint16(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no status flags")
	}

	// Upper 2 bytes of the capability flags.
	capUpper, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no capability flags (upper 2 bytes)")
	}
	capabilities += uint32(capUpper) << 16

	// Length of auth-plugin-Content, or 0.
	// Only with CLIENT_PLUGIN_AUTH capability.
	var authPluginDataLength byte
	if capabilities&mysql.CapabilityClientPluginAuth != 0 {
		authPluginDataLength, pos, ok = readByte(data, pos)
		if !ok {
			return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-Content")
		}
	} else {
		// One byte filler, 0. We don't really care about the value.
		_, pos, ok = readByte(data, pos)
		if !ok {
			return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no length of auth-plugin-Content filler")
		}
	}

	// 10 reserved 0 bytes.
	pos += 10

	if capabilities&mysql.CapabilityClientSecureConnection != 0 {
		// The next part of the auth-plugin-Content.
		// The length is max(13, length of auth-plugin-Content - 8).
		l := int(authPluginDataLength) - 8
		if l > 13 {
			l = 13
		}
		var authPluginDataPart2 []byte
		authPluginDataPart2, pos, ok = readBytes(data, pos, l)
		if !ok {
			return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: packet has no auth-plugin-Content-part-2")
		}

		// The last byte has to be 0, and is not part of the Content.
		if authPluginDataPart2[l-1] != 0 {
			return 0, nil, "", err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "parseInitialHandshakePacket: auth-plugin-Content-part-2 is not 0 terminated")
		}
		authPluginData = append(authPluginData, authPluginDataPart2[0:l-1]...)
	}

	// Auth-plugin name.
	if capabilities&mysql.CapabilityClientPluginAuth != 0 {
		authPluginName, _, ok := readNullString(data, pos)
		if !ok {
			// Fallback for versions prior to 5.5.10 and
			// 5.6.2 that don't have a null terminated string.
			authPluginName = string(data[pos : len(data)-1])
		}

		return capabilities, authPluginData, authPluginName, nil
	}

	return capabilities, authPluginData, mysql.MysqlNativePassword, nil
}

// writeHandshakeResponse41 writes the handshake response.
// Returns a SQLError.
func (conn *BackendConnection) writeHandshakeResponse41(capabilities uint32, scrambledPassword []byte, plugin string) error {
	// Build our flags.
	var flags = mysql.CapabilityClientLongPassword |
		mysql.CapabilityClientLongFlag |
		mysql.CapabilityClientProtocol41 |
		mysql.CapabilityClientTransactions |
		mysql.CapabilityClientSecureConnection |
		mysql.CapabilityClientMultiStatements |
		mysql.CapabilityClientMultiResults |
		mysql.CapabilityClientPluginAuth |
		mysql.CapabilityClientPluginAuthLenencClientData |
		// If the server supported
		// CapabilityClientDeprecateEOF, we also support it.
		conn.capabilities&mysql.CapabilityClientDeprecateEOF

	if conn.conf.ClientFoundRows {
		// Pass-through ClientFoundRows flag.
		flags |= mysql.CapabilityClientFoundRows
	}

	// FIXME(alainjobart) add multi statement.

	length :=
		4 + // Client capability flags.
			4 + // Max-packet size.
			1 + // Character set.
			23 + // Reserved.
			lenNullString(conn.conf.User) +
			// length of scrambled password is handled below.
			len(scrambledPassword) +
			21 + // "mysql_native_password" string.
			1 // terminating zero.

	// Add the DB name if the server supports it.
	if conn.conf.DBName != "" && (capabilities&mysql.CapabilityClientConnectWithDB != 0) {
		flags |= mysql.CapabilityClientConnectWithDB
		length += lenNullString(conn.conf.DBName)
	}

	if capabilities&mysql.CapabilityClientPluginAuthLenencClientData != 0 {
		length += lenEncIntSize(uint64(len(scrambledPassword)))
	} else {
		length++
	}

	data := conn.c.startEphemeralPacket(length)
	pos := 0

	// Client capability flags.
	pos = writeUint32(data, pos, flags)

	// Max-packet size, always 0. See doc.go.
	pos = writeZeroes(data, pos, 4)

	// Character set.
	pos = writeByte(data, pos, byte(mysql.Collations[conn.conf.Collation]))

	// 23 reserved bytes, all 0.
	pos = writeZeroes(data, pos, 23)

	// Username
	pos = writeNullString(data, pos, conn.conf.User)

	// Scrambled password.  The length is encoded as variable length if
	// CapabilityClientPluginAuthLenencClientData is set.
	if capabilities&mysql.CapabilityClientPluginAuthLenencClientData != 0 {
		pos = writeLenEncInt(data, pos, uint64(len(scrambledPassword)))
	} else {
		data[pos] = byte(len(scrambledPassword))
		pos++
	}
	pos += copy(data[pos:], scrambledPassword)

	// DbName, only if server supports it.
	if conn.conf.DBName != "" && (capabilities&mysql.CapabilityClientConnectWithDB != 0) {
		pos = writeNullString(data, pos, conn.conf.DBName)
	}

	// Assume native client during response
	pos = writeNullString(data, pos, plugin)

	// Sanity-check the length.
	if pos != len(data) {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "writeHandshakeResponse41: only packed %v bytes, out of %v allocated", pos, len(data))
	}

	if err := conn.c.writeEphemeralPacket(); err != nil {
		return err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "cannot send HandshakeResponse41: %v", err)
	}
	return nil
}

// WriteComInitDB changes the default database to use.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComInitDB(db string) error {
	data := conn.c.startEphemeralPacket(len(db) + 1)
	data[0] = mysql.ComInitDB
	copy(data[1:], db)
	if err := conn.c.writeEphemeralPacket(); err != nil {
		return err2.NewSQLError(mysql.CRServerGone, mysql.SSUnknownSQLState, err.Error())
	}
	return nil
}

// WriteComQuery writes a query for the server to execute.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComQuery(query string) error {
	// This is a new command, need to reset the sequence.
	conn.c.sequence = 0

	data := conn.c.startEphemeralPacket(len(query) + 1)
	data[0] = mysql.ComQuery
	copy(data[1:], query)
	if err := conn.c.writeEphemeralPacket(); err != nil {
		return err2.NewSQLError(mysql.CRServerGone, mysql.SSUnknownSQLState, err.Error())
	}
	return nil
}

// WriteComSetOption changes the connection's capability of executing multi statements.
// Returns SQLError(CRServerGone) if it can't.
func (conn *BackendConnection) WriteComSetOption(operation uint16) error {
	data := conn.c.startEphemeralPacket(16 + 1)
	data[0] = mysql.ComSetOption
	writeUint16(data, 1, operation)
	if err := conn.c.writeEphemeralPacket(); err != nil {
		return err2.NewSQLError(mysql.CRServerGone, mysql.SSUnknownSQLState, err.Error())
	}
	return nil
}

func (conn *BackendConnection) WriteComFieldList(table string, wildcard string) error {
	conn.c.sequence = 0
	length := lenNullString(table) + lenNullString(wildcard)
	if len(wildcard) > 0 {
		length++
	}

	data := conn.c.startEphemeralPacket(length)
	pos := 0

	pos = writeByte(data, 0, mysql.ComFieldList)
	if len(wildcard) > 0 {
		pos = writeNullString(data, pos, table)
		writeNullString(data, pos, wildcard)
	} else {
		pos = writeEOFString(data, pos, table)
		writeEOFString(data, pos, wildcard)
	}

	if err := conn.c.writeEphemeralPacket(); err != nil {
		return err
	}

	return nil
}

func (conn *BackendConnection) readResultSetHeaderPacket() (affectedRows, lastInsertID uint64, colNumber int, more bool, warnings uint16, err error) {
	// Get the result.
	affectedRows, lastInsertID, colNumber, more, warning, err := conn.readComQueryResponse()
	if err != nil {
		return affectedRows, lastInsertID, colNumber, more, warning, err
	}
	return affectedRows, lastInsertID, colNumber, more, warning, nil
}

func (conn *BackendConnection) readResultSetColumnsPacket(colNumber int) (columns []proto.Field, err error) {
	// Read column headers. One packet per column.
	// Build the fields.
	columns = make([]proto.Field, colNumber)
	for i := 0; i < colNumber; i++ {
		field := &Field{}
		columns[i] = field
		if err = conn.ReadColumnDefinition(field, i); err != nil {
			return
		}
	}
	return
}

// ReadQueryRow returns iterator, and the line reads the results set
func (conn *BackendConnection) ReadQueryRow() *RawResult {
	return newResult(conn)
}

// ReadQueryResult gets the result from the last written query.
func (conn *BackendConnection) ReadQueryResult(wantFields bool) proto.Result {
	ret := conn.ReadQueryRow()
	ret.setWantFields(wantFields)
	return ret
}

func (conn *BackendConnection) readComQueryResponse() (affectedRows uint64, lastInsertID uint64, status int, more bool, warnings uint16, err error) {
	data, err := conn.c.readEphemeralPacket()
	if err != nil {
		return 0, 0, 0, false, 0, err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", err)
	}
	defer conn.c.recycleReadPacket()
	if len(data) == 0 {
		return 0, 0, 0, false, 0, err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "invalid empty COM_QUERY response packet")
	}

	switch data[0] {
	case mysql.OKPacket:
		affectedRows, lastInsertID, status, warnings, err := parseOKPacket(data)
		return affectedRows, lastInsertID, 0, (status & mysql.ServerMoreResultsExists) != 0, warnings, err
	case mysql.ErrPacket:
		// Error
		return 0, 0, 0, false, 0, ParseErrorPacket(data)
	case 0xfb:
		// Local infile
		return 0, 0, 0, false, 0, fmt.Errorf("not implemented")
	}
	n, pos, ok := readLenEncInt(data, 0)
	if !ok {
		return 0, 0, 0, false, 0, err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "cannot get column number")
	}
	if pos != len(data) {
		return 0, 0, 0, false, 0, err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extra Content in COM_QUERY response")
	}
	return 0, 0, int(n), false, 0, nil
}

// ReadColumnDefinition reads the next Column Definition packet.
// Returns a SQLError.
func (conn *BackendConnection) ReadColumnDefinition(field *Field, index int) error {
	colDef, err := conn.c.readEphemeralPacket()
	if err != nil {
		return err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", err)
	}
	defer conn.c.recycleReadPacket()

	if isEOFPacket(colDef) {
		return io.EOF
	}

	// Catalog is ignored, always set to "def"
	pos, ok := skipLenEncString(colDef, 0)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v catalog failed", index)
	}

	// schema, table, orgTable, name and OrgName are strings.
	field.database, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v schema failed", index)
	}
	field.table, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v table failed", index)
	}
	field.orgTable, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v org_table failed", index)
	}
	field.name, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v name failed", index)
	}
	field.orgName, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	characterSet, pos, ok := readUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v characterSet failed", index)
	}
	field.charSet = characterSet

	// columnLength is a uint32.
	field.columnLength, pos, ok = readUint32(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v columnLength failed", index)
	}

	// type is one byte.
	t, pos, ok := readByte(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v type failed", index)
	}

	// flags is 2 bytes.
	flags, pos, ok := readUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v flags failed", index)
	}
	field.flags = uint(flags)

	// Convert MySQL type to Vitess type.
	field.fieldType, err = mysql.MySQLToType(int64(t), int64(flags))
	if err != nil {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}
	// Decimals is a byte.
	decimals, pos, ok := readByte(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v decimals failed", index)
	}
	field.decimals = decimals

	//if more Content, command was field list
	if len(colDef) > pos+8 {
		//length of default value lenenc-int
		field.defaultValueLength, pos, ok = readUint64(colDef, pos)
		if !ok {
			return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v default value failed", index)
		}

		if pos+int(field.defaultValueLength) > len(colDef) {
			return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v default value failed", index)
		}

		//default value string[$len]
		field.defaultValue = colDef[pos:(pos + int(field.defaultValueLength))]
	}
	return nil
}

// ReadColumnDefinitionType is a faster version of
// ReadColumnDefinition that only fills in the Type.
// Returns a SQLError.
func (conn *BackendConnection) ReadColumnDefinitionType(field *Field, index int) error {
	colDef, err := conn.c.readEphemeralPacket()
	if err != nil {
		return err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", err)
	}
	defer conn.c.recycleReadPacket()

	// catalog, schema, table, orgTable, name and orgName are
	// strings, all skipped.
	pos, ok := skipLenEncString(colDef, 0)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v catalog failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v schema failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v table failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v org_table failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v name failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "skipping col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	_, pos, ok = readUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v characterSet failed", index)
	}

	// columnLength is a uint32.
	_, pos, ok = readUint32(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v columnLength failed", index)
	}

	// type is one byte
	t, pos, ok := readByte(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v type failed", index)
	}

	// flags is 2 bytes
	flags, _, ok := readUint16(colDef, pos)
	if !ok {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "extracting col %v flags failed", index)
	}

	// Convert MySQL type to Vitess type.
	field.fieldType, err = mysql.MySQLToType(int64(t), int64(flags))
	if err != nil {
		return err2.NewSQLError(mysql.CRMalformedPacket, mysql.SSUnknownSQLState, "MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}

	// skip decimals

	return nil
}

// DrainResults will read all packets for a result set and ignore them.
func (conn *BackendConnection) DrainResults() error {
	for {
		data, err := conn.c.readEphemeralPacket()
		if err != nil {
			return err2.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", err)
		}
		if isEOFPacket(data) {
			conn.c.recycleReadPacket()
			return nil
		}

		if isErrorPacket(data) {
			err = ParseErrorPacket(data)
			conn.c.recycleReadPacket()
			return err
		}
		conn.c.recycleReadPacket()
	}
}

func (conn *BackendConnection) ReadColumnDefinitions() ([]proto.Field, error) {
	result := make([]proto.Field, 0)
	i := 0
	for {
		field := &Field{}
		err := conn.ReadColumnDefinition(field, i)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		result = append(result, field)
		i++
	}
}

// ExecuteWithWarningCountIterRow is for fetching results and a warning count
// Note: In a future iteration this should be abolished and merged into the Execute API.
func (conn *BackendConnection) ExecuteWithWarningCountIterRow(query string) (result proto.Result, err error) {
	defer func() {
		if err != nil {
			if sqlErr, ok := err.(*err2.SQLError); ok {
				sqlErr.Query = query
			}
		}
	}()

	// Send the query as a COM_QUERY packet.
	if err = conn.WriteComQuery(query); err != nil {
		return
	}

	res := conn.ReadQueryRow()
	res.setTextProtocol()
	res.setWantFields(true)

	result = res

	return
}

// ExecuteWithWarningCount is for fetching results and a warning count
// Note: In a future iteration this should be abolished and merged into the
// Execute API.
func (conn *BackendConnection) ExecuteWithWarningCount(query string, wantFields bool) (result proto.Result, err error) {
	defer func() {
		if err != nil {
			if sqlErr, ok := err.(*err2.SQLError); ok {
				sqlErr.Query = query
			}
		}
	}()

	// Send the query as a COM_QUERY packet.
	if err = conn.WriteComQuery(query); err != nil {
		return
	}

	result = conn.ReadQueryResult(wantFields)

	return
}

func (conn *BackendConnection) PrepareExecuteArgs(query string, args []interface{}) (proto.Result, error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.execArgs(args)
}

func (conn *BackendConnection) PrepareQueryArgs(query string, data []interface{}) (proto.Result, error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.queryArgs(data)
}

func (conn *BackendConnection) PrepareExecute(query string, data []byte) (proto.Result, error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.exec(data)
}

func (conn *BackendConnection) PrepareQuery(query string, data []byte) (proto.Result, error) {
	stmt, err := conn.prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.query(data)
}

func (conn *BackendConnection) prepare(query string) (*BackendStatement, error) {
	// This is a new command, need to reset the sequence.
	conn.c.sequence = 0

	data := conn.c.startEphemeralPacket(len(query) + 1)
	data[0] = mysql.ComPrepare
	copy(data[1:], query)
	if err := conn.c.writeEphemeralPacket(); err != nil {
		return nil, err2.NewSQLError(mysql.CRServerGone, mysql.SSUnknownSQLState, err.Error())
	}

	stmt := &BackendStatement{
		conn: conn,
		sql:  query,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = conn.DrainResults(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = conn.DrainResults()
		}
	}

	return stmt, err
}

func (conn *BackendConnection) Close() {
	conn.c.Close()
}

func (conn *BackendConnection) GetDatabaseConn() *Conn {
	return conn.c
}
