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

//go:generate mockgen -destination=../../testdata/mock_interface.go -package=testdata . FrontConn
package proto

import (
	"context"
	"sort"
)

import (
	"github.com/arana-db/arana/pkg/util/bytesconv"
)

type (
	ContextKeyTenant                 struct{}
	ContextKeySchema                 struct{}
	ContextKeySQL                    struct{}
	ContextKeyTransientVariables     struct{}
	ContextKeyServerVersion          struct{}
	ContextKeyEnableLocalComputation struct{}
)

type (
	// FrontConn represents a frontend connection.
	//    APP ---> FRONTEND_CONN ---> ARANA ---> BACKEND_CONN ---> MySQL
	FrontConn interface {
		// ID returns connection id.
		ID() uint32

		// Schema returns the current schema.
		Schema() string

		// SetSchema sets the current schema.
		SetSchema(schema string)

		// Tenant returns the tenant.
		Tenant() string

		// SetTenant sets the tenant.
		SetTenant(tenant string)

		// TransientVariables returns the transient variables.
		TransientVariables() map[string]Value

		// SetTransientVariables sets the transient variables.
		SetTransientVariables(v map[string]Value)

		// CharacterSet returns the character set.
		CharacterSet() uint8

		// ServerVersion returns the server version.
		ServerVersion() string
	}

	// Context is used to carry context objects
	Context struct {
		context.Context
		C FrontConn

		// sql Data
		Data []byte

		Stmt *Stmt
	}

	Listener interface {
		SetExecutor(executor Executor)
		Listen()
		Close()
	}

	Executor interface {
		ProcessDistributedTransaction() bool
		InLocalTransaction(ctx *Context) bool
		InGlobalTransaction(ctx *Context) bool
		ExecuteUseDB(ctx *Context, schema string) error
		ExecuteFieldList(ctx *Context) ([]Field, error)
		ExecutorComQuery(ctx *Context, callback func(Result, uint16, error) error) error
		ExecutorComStmtExecute(ctx *Context) (Result, uint16, error)
		ConnectionClose(ctx *Context)
	}
)

func (c Context) GetQuery() string {
	if c.Stmt != nil {
		if len(c.Stmt.PrepareStmt) > 0 {
			return c.Stmt.PrepareStmt
		}
		if c.Stmt.StmtNode != nil {
			return c.Stmt.StmtNode.Text()
		}
	}
	return bytesconv.BytesToString(c.Data[1:])
}

func (c Context) GetArgs() []Value {
	if c.Stmt == nil || len(c.Stmt.BindVars) < 1 {
		return nil
	}

	var (
		keys = make([]string, 0, len(c.Stmt.BindVars))
		args = make([]Value, 0, len(c.Stmt.BindVars))
	)

	for k := range c.Stmt.BindVars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, c.Stmt.BindVars[k])
	}
	return args
}

func (c Context) Value(key interface{}) interface{} {
	switch key.(type) {
	case ContextKeyTenant:
		return c.C.Tenant()
	case ContextKeySchema:
		return c.C.Schema()
	case ContextKeyTransientVariables:
		return c.C.TransientVariables()
	case ContextKeySQL:
		return c.GetQuery()
	case ContextKeyServerVersion:
		return c.C.ServerVersion()
	case ContextKeyEnableLocalComputation:
		return c.Context.Value(ContextKeyEnableLocalComputation{})
	}
	return c.Context.Value(key)
}
