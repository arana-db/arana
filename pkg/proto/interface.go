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

package proto

import (
	"context"
	"sort"
)

import (
	"github.com/arana-db/arana/pkg/util/bytesconv"
)

type (
	ContextKeyTenant             struct{}
	ContextKeySchema             struct{}
	ContextKeySQL                struct{}
	ContextKeyTransientVariables struct{}
	ContextKeyServerVersion      struct{}
)

type (

	// Context is used to carry context objects
	Context struct {
		context.Context
		Tenant        string
		Schema        string
		ServerVersion string

		ConnectionID uint32

		// sql Data
		Data []byte

		Stmt *Stmt

		// TransientVariables stores the transient local variables, it will sync with the remote node automatically.
		//   - SYSTEM: @@xxx
		//   - USER: @xxx
		TransientVariables map[string]Value
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
		ExecuteUseDB(ctx *Context) error
		ExecuteFieldList(ctx *Context) ([]Field, error)
		ExecutorComQuery(ctx *Context) (Result, uint16, error)
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
		return c.Tenant
	case ContextKeySchema:
		return c.Schema
	case ContextKeyTransientVariables:
		return c.TransientVariables
	case ContextKeySQL:
		return c.GetQuery()
	case ContextKeyServerVersion:
		return c.ServerVersion
	}
	return c.Context.Value(key)
}
