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
	"strings"
)

import (
	"github.com/arana-db/parser"
)

import (
	"github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/util/log"
)

func (l *Listener) handleInitDB(c *Conn, ctx *proto.Context) error {
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
	if err = c.writeOKPacket(0, 0, c.StatusFlags, 0); err != nil {
		log.Errorf("Error writing ComInitDB result to %s: %v", c, err)
		return err
	}

	return nil
}

func (l *Listener) handleQuery(c *Conn, ctx *proto.Context) error {
	return func() error {
		c.startWriterBuffering()
		defer func() {
			if err := c.endWriterBuffering(); err != nil {
				log.Errorf("conn %v: flush() failed: %v", c.ID(), err)
			}
		}()

		c.recycleReadPacket()
		result, warn, err := l.executor.ExecutorComQuery(ctx)
		if err != nil {
			if wErr := c.writeErrorPacketFromError(err); wErr != nil {
				log.Error("Error writing query error to client %v: %v", l.connectionID, wErr)
				return wErr
			}
			return nil
		}
		if len(result.GetFields()) == 0 {
			// A successful callback with no fields means that this was a
			// DML or other write-only operation.
			//
			// We should not send any more packets after this, but make sure
			// to extract the affected rows and last insert id from the result
			// struct here since clients expect it.
			var (
				affected, _ = result.RowsAffected()
				insertId, _ = result.LastInsertId()
			)
			return c.writeOKPacket(affected, insertId, c.StatusFlags, warn)
		}
		if err = c.writeFields(l.capabilities, result); err != nil {
			return err
		}
		if err = c.writeRows(result); err != nil {
			return err
		}
		if err = c.writeEndResult(l.capabilities, false, 0, 0, warn); err != nil {
			log.Errorf("Error writing result to %s: %v", c, err)
			return err
		}
		return nil
	}()
}

func (l *Listener) handleFieldList(c *Conn, ctx *proto.Context) error {
	c.recycleReadPacket()
	fields, err := l.executor.ExecuteFieldList(ctx)
	if err != nil {
		log.Errorf("Conn %v: Error write field list: %v", c, err)
		if wErr := c.writeErrorPacketFromError(err); wErr != nil {
			// If we can't even write the error, we're done.
			log.Errorf("Conn %v: Error write field list error: %v", c, wErr)
			return wErr
		}
	}
	return c.writeFields(l.capabilities, &Result{Fields: fields})
}

func (l *Listener) handleStmtExecute(c *Conn, ctx *proto.Context) error {
	return func() error {
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
			if wErr := c.writeErrorPacketFromError(err); wErr != nil {
				// If we can't even write the error, we're done.
				log.Error("Error writing query error to client %v: %v", l.connectionID, wErr)
				return wErr
			}
			return nil
		}

		prepareStmt, _ := l.stmts.Load(stmtID)
		ctx.Stmt = prepareStmt.(*proto.Stmt)

		result, warn, err := l.executor.ExecutorComStmtExecute(ctx)
		if err != nil {
			if wErr := c.writeErrorPacketFromError(err); wErr != nil {
				log.Error("Error writing query error to client %v: %v", l.connectionID, wErr)
				return wErr
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
		if err = c.writeFields(l.capabilities, result); err != nil {
			return err
		}
		if err = c.writeBinaryRows(result); err != nil {
			return err
		}
		if err = c.writeEndResult(l.capabilities, false, 0, 0, warn); err != nil {
			log.Errorf("Error writing result to %s: %v", c, err)
			return err
		}
		return nil
	}()
}

func (l *Listener) handlePrepare(c *Conn, ctx *proto.Context) error {
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
		if wErr := c.writeErrorPacketFromError(err); wErr != nil {
			log.Errorf("Conn %v: Error writing prepared statement error: %v", c, wErr)
			return wErr
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

	return c.writePrepare(l.capabilities, stmt)
}

func (l *Listener) handleStmtReset(c *Conn, ctx *proto.Context) error {
	stmtID, _, ok := readUint32(ctx.Data, 1)
	c.recycleReadPacket()
	if ok {
		if prepare, ok := l.stmts.Load(stmtID); ok {
			prepareStmt, _ := prepare.(*proto.Stmt)
			prepareStmt.BindVars = make(map[string]interface{})
		}
	}
	return c.writeOKPacket(0, 0, c.StatusFlags, 0)
}

func (l *Listener) handleSetOption(c *Conn, ctx *proto.Context) error {
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
	}
	log.Errorf("Got unhandled packet (ComSetOption else) from client %v, returning error: %v", l.connectionID, ctx.Data)
	if err := c.writeErrorPacket(mysql.ERUnknownComError, mysql.SSUnknownComError, "error handling packet: %v", ctx.Data); err != nil {
		log.Errorf("Error writing error packet to client: %v", err)
		return err
	}
	return nil
}
