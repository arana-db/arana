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
	"io"
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	merrors "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	_flagTextMode uint8 = 1 << iota
	_flagWantFields
)

var (
	_ proto.Result  = (*RawResult)(nil)
	_ proto.Dataset = (*RawResult)(nil)
)

type RawResult struct {
	flag uint8

	c *BackendConnection

	lastInsertID, affectedRows uint64
	colNumber                  int
	more                       bool
	warnings                   uint16
	fields                     []proto.Field

	preflightOnce     sync.Once
	preflightFailure  error
	flightOnce        sync.Once
	flightFailure     error
	postFlightOnce    sync.Once
	postFlightFailure error

	closeFunc    func() error
	closeOnce    sync.Once
	closeFailure error

	eof bool
}

func (rr *RawResult) Discard() (err error) {
	defer func() {
		_ = rr.Close()
	}()

	if err = rr.postFlight(); err != nil {
		return errors.Wrapf(err, "failed to discard mysql result")
	}

	if rr.colNumber < 1 {
		return
	}

	for {
		_, err = rr.nextRowData()
		if err != nil {
			return
		}
	}
}

func (rr *RawResult) Warn() (uint16, error) {
	if err := rr.preflight(); err != nil {
		return 0, err
	}
	return rr.warnings, nil
}

func (rr *RawResult) Close() error {
	rr.closeOnce.Do(func() {
		if rr.closeFunc == nil {
			return
		}
		rr.closeFailure = rr.closeFunc()
		if rr.closeFailure != nil {
			log.Errorf("failed to close mysql result: %v", rr.closeFailure)
		}
	})
	return rr.closeFailure
}

func (rr *RawResult) Fields() ([]proto.Field, error) {
	if err := rr.flight(); err != nil {
		return nil, err
	}
	return rr.fields, nil
}

func (rr *RawResult) Next() (row proto.Row, err error) {
	defer func() {
		if err != nil {
			_ = rr.Close()
		}
	}()

	var data []byte
	if data, err = rr.nextRowData(); err != nil {
		return
	}

	if rr.flag&_flagTextMode != 0 {
		row = TextRow{
			fields: rr.fields,
			raw:    data,
		}
	} else {
		row = BinaryRow{
			fields: rr.fields,
			raw:    data,
		}
	}
	return
}

func (rr *RawResult) Dataset() (proto.Dataset, error) {
	if err := rr.postFlight(); err != nil {
		return nil, err
	}

	if len(rr.fields) < 1 {
		return nil, nil
	}

	return rr, nil
}

func (rr *RawResult) preflight() (err error) {
	defer func() {
		if err != nil || rr.colNumber < 1 {
			_ = rr.Close()
		}
	}()

	rr.preflightOnce.Do(func() {
		rr.affectedRows, rr.lastInsertID, rr.colNumber, rr.more, rr.warnings, rr.preflightFailure = rr.c.readResultSetHeaderPacket()
	})
	err = rr.preflightFailure
	return
}

func (rr *RawResult) flight() (err error) {
	if err = rr.preflight(); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = rr.Close()
		}
	}()

	rr.flightOnce.Do(func() {
		if rr.colNumber < 1 {
			return
		}
		columns := make([]proto.Field, rr.colNumber)
		for i := 0; i < rr.colNumber; i++ {
			var field Field

			if rr.flag&_flagWantFields != 0 {
				rr.flightFailure = rr.c.ReadColumnDefinition(&field, i)
			} else {
				rr.flightFailure = rr.c.ReadColumnDefinitionType(&field, i)
			}

			if rr.flightFailure != nil {
				return
			}
			columns[i] = &field
		}

		rr.fields = columns
	})

	err = rr.flightFailure

	return
}

func (rr *RawResult) postFlight() (err error) {
	if err = rr.flight(); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = rr.Close()
		}
	}()

	rr.postFlightOnce.Do(func() {
		if rr.colNumber < 1 {
			return
		}

		if rr.c.capabilities&mysql.CapabilityClientDeprecateEOF != 0 {
			return
		}

		var data []byte

		// EOF is only present here if it's not deprecated.
		if data, rr.postFlightFailure = rr.c.c.readEphemeralPacket(); rr.postFlightFailure != nil {
			rr.postFlightFailure = merrors.NewSQLError(mysql.CRServerLost, mysql.SSUnknownSQLState, "%v", rr.postFlightFailure)
			return
		}

		if isEOFPacket(data) {
			// This is what we expect.
			// Warnings and status flags are ignored.
			rr.c.c.recycleReadPacket()
			// goto: read row loop
			return
		}

		defer rr.c.c.recycleReadPacket()

		if isErrorPacket(data) {
			rr.postFlightFailure = ParseErrorPacket(data)
		} else {
			rr.postFlightFailure = errors.Errorf("unexpected packet after fields: %v", data)
		}
	})

	err = rr.postFlightFailure

	return
}

func (rr *RawResult) LastInsertId() (uint64, error) {
	if err := rr.preflight(); err != nil {
		return 0, err
	}
	return rr.lastInsertID, nil
}

func (rr *RawResult) RowsAffected() (uint64, error) {
	if err := rr.preflight(); err != nil {
		return 0, err
	}
	return rr.affectedRows, nil
}

func (rr *RawResult) nextRowData() (data []byte, err error) {
	if rr.eof {
		err = io.EOF
		return
	}

	if data, err = rr.c.c.readPacket(); err != nil {
		return
	}

	switch {
	case isEOFPacket(data):
		rr.eof = true
		// The deprecated EOF packets change means that this is either an
		// EOF packet or an OK packet with the EOF type code.
		if rr.c.capabilities&mysql.CapabilityClientDeprecateEOF == 0 {
			if _, rr.more, err = parseEOFPacket(data); err != nil {
				return
			}
		} else {
			var statusFlags uint16
			if _, _, statusFlags, _, err = parseOKPacket(data); err != nil {
				return
			}
			rr.more = statusFlags&mysql.ServerMoreResultsExists != 0
		}
		data = nil
		err = io.EOF
	case isErrorPacket(data):
		rr.eof = true
		data = nil
		err = ParseErrorPacket(data)
	}

	// TODO: Check we're not over the limit before we add more.
	//if len(result.Rows) == maxrows {
	//	if err := conn.DrainResults(); err != nil {
	//		return nil, false, 0, err
	//	}
	//	return nil, false, 0, err2.NewSQLError(mysql.ERVitessMaxRowsExceeded, mysql.SSUnknownSQLState, "Row count exceeded %d")
	//}

	return
}

func (rr *RawResult) setTextProtocol() {
	rr.flag |= _flagTextMode
}

func (rr *RawResult) setBinaryProtocol() {
	rr.flag &= ^_flagTextMode
}

func (rr *RawResult) setWantFields(b bool) {
	if b {
		rr.flag |= _flagWantFields
	} else {
		rr.flag &= ^_flagWantFields
	}
}

func (rr *RawResult) SetCloser(closer func() error) {
	rr.closeFunc = closer
}

func newResult(c *BackendConnection) *RawResult {
	return &RawResult{c: c}
}
