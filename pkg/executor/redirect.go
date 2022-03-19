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

package executor

import (
	"bytes"
	stdErrors "errors"
	"sync"
)

import (
	"github.com/arana-db/parser"
	"github.com/arana-db/parser/ast"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
)

var errMissingTx = stdErrors.New("no transaction found")

// IsErrMissingTx returns true if target error was caused by missing-tx.
func IsErrMissingTx(err error) bool {
	return errors.Is(err, errMissingTx)
}

type RedirectExecutor struct {
	preFilters          []proto.PreFilter
	postFilters         []proto.PostFilter
	localTransactionMap sync.Map // map[uint32]proto.Tx, (ConnectionID,Tx)
}

func NewRedirectExecutor() *RedirectExecutor {
	return &RedirectExecutor{}
}

func (executor *RedirectExecutor) AddPreFilter(filter proto.PreFilter) {
	executor.preFilters = append(executor.preFilters, filter)
}

func (executor *RedirectExecutor) AddPostFilter(filter proto.PostFilter) {
	executor.postFilters = append(executor.postFilters, filter)
}

func (executor *RedirectExecutor) GetPreFilters() []proto.PreFilter {
	return executor.preFilters
}

func (executor *RedirectExecutor) GetPostFilters() []proto.PostFilter {
	return executor.postFilters
}

func (executor *RedirectExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *RedirectExecutor) InLocalTransaction(ctx *proto.Context) bool {
	_, ok := executor.localTransactionMap.Load(ctx.ConnectionID)
	return ok
}

func (executor *RedirectExecutor) InGlobalTransaction(ctx *proto.Context) bool {
	return false
}

func (executor *RedirectExecutor) ExecuteUseDB(ctx *proto.Context) error {
	// TODO: check permission, target database should belong to same tenant.
	// TODO: process transactions when database switched?

	// do nothing.
	//resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
	//r, err := resourcePool.Get(ctx)
	//defer func() {
	//	resourcePool.Put(r)
	//}()
	//if err != nil {
	//	return err
	//}
	//backendConn := r.(*mysql.BackendConnection)
	//db := string(ctx.Data[1:])
	//return backendConn.WriteComInitDB(db)
	return nil
}

func (executor *RedirectExecutor) ExecuteFieldList(ctx *proto.Context) ([]proto.Field, error) {
	index := bytes.IndexByte(ctx.Data, 0x00)
	table := string(ctx.Data[0:index])
	wildcard := string(ctx.Data[index+1:])

	rt, err := runtime.Load(ctx.Schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	db := rt.Namespace().DB0(ctx.Context)
	if db == nil {
		return nil, errors.New("cannot get physical backend connection")
	}

	return db.CallFieldList(ctx.Context, table, wildcard)
}

func (executor *RedirectExecutor) ExecutorComQuery(ctx *proto.Context) (proto.Result, uint16, error) {
	var err error

	p := parser.New()
	query := ctx.GetQuery()
	act, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, 0, err
	}
	log.Debugf("ComQuery: %s", query)

	ctx.Stmt = &proto.Stmt{
		StmtNode: act,
	}

	rt, err := runtime.Load(ctx.Schema)
	if err != nil {
		return nil, 0, err
	}

	var (
		res  proto.Result
		warn uint16
	)

	executor.doPreFilter(ctx)

	switch act.(type) {
	case *ast.BeginStmt:
		// begin a new tx
		var tx proto.Tx
		if tx, err = rt.Begin(ctx); err == nil {
			executor.putTx(ctx, tx)
			res = &mysql.Result{}
		}
	case *ast.CommitStmt:
		// remove existing tx, and commit it
		if tx, ok := executor.removeTx(ctx); ok {
			res, warn, err = tx.Commit(ctx.Context)
		} else {
			res, warn, err = nil, 0, errMissingTx
		}
	case *ast.RollbackStmt:
		// remove existing tx, and rollback it
		if tx, ok := executor.removeTx(ctx); ok {
			res, warn, err = tx.Rollback(ctx.Context)
		} else {
			res, warn, err = nil, 0, errMissingTx
		}
	case *ast.SelectStmt:
		// TODO: merge with other stmt when write-mode is supported for runtime
		if tx, ok := executor.getTx(ctx); ok {
			res, warn, err = tx.Execute(ctx)
		} else {
			res, warn, err = rt.Execute(ctx)
		}
	default:
		// TODO: mark direct flag temporarily, remove when write-mode is supported for runtime
		ctx.Context = rcontext.WithDirect(ctx.Context)
		if tx, ok := executor.getTx(ctx); ok {
			res, warn, err = tx.Execute(ctx)
		} else {
			res, warn, err = rt.Execute(ctx)
		}
	}

	executor.doPostFilter(ctx, res)

	return res, warn, err
}

func (executor *RedirectExecutor) ExecutorComStmtExecute(ctx *proto.Context) (proto.Result, uint16, error) {
	var (
		executable proto.Executable
		result     proto.Result
		warn       uint16
		err        error
	)

	if tx, ok := executor.getTx(ctx); ok {
		executable = tx
	} else {
		var rt runtime.Runtime
		if rt, err = runtime.Load(ctx.Schema); err != nil {
			return nil, 0, err
		}
		executable = rt
	}

	switch ctx.Stmt.StmtNode.(type) {
	case *ast.SelectStmt:
	default:
		ctx.Context = rcontext.WithDirect(ctx.Context)
	}

	query := ctx.Stmt.StmtNode.Text()
	log.Debugf(query)

	executor.doPreFilter(ctx)
	result, warn, err = executable.Execute(ctx)
	executor.doPostFilter(ctx, result)
	return result, warn, err
}

func (executor *RedirectExecutor) ConnectionClose(ctx *proto.Context) {
	tx, ok := executor.removeTx(ctx)
	if !ok {
		return
	}
	if _, _, err := tx.Rollback(ctx); err != nil {
		log.Errorf("failed to rollback tx: %s", err)
	}

	//resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
	//r, ok := executor.localTransactionMap[ctx.ConnectionID]
	//if ok {
	//	defer func() {
	//		resourcePool.Put(r)
	//	}()
	//	backendConn := r.(*mysql.BackendConnection)
	//	_, _, err := backendConn.ExecuteWithWarningCount("rollback", true)
	//	if err != nil {
	//		log.Error(err)
	//	}
	//}
}

func (executor *RedirectExecutor) putTx(ctx *proto.Context, tx proto.Tx) {
	executor.localTransactionMap.Store(ctx.ConnectionID, tx)
}

func (executor *RedirectExecutor) removeTx(ctx *proto.Context) (proto.Tx, bool) {
	exist, ok := executor.localTransactionMap.LoadAndDelete(ctx.ConnectionID)
	if !ok {
		return nil, false
	}
	return exist.(proto.Tx), true
}

func (executor *RedirectExecutor) getTx(ctx *proto.Context) (proto.Tx, bool) {
	exist, ok := executor.localTransactionMap.Load(ctx.ConnectionID)
	if !ok {
		return nil, false
	}
	return exist.(proto.Tx), true
}

func (executor *RedirectExecutor) doPreFilter(ctx *proto.Context) {
	for i := 0; i < len(executor.preFilters); i++ {
		func(ctx *proto.Context) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("failed to execute filter: %s, err: %v", executor.preFilters[i].GetName(), err)
				}
			}()
			filter := executor.preFilters[i]
			filter.PreHandle(ctx)
		}(ctx)
	}
}

func (executor *RedirectExecutor) doPostFilter(ctx *proto.Context, result proto.Result) {
	for i := 0; i < len(executor.postFilters); i++ {
		func(ctx *proto.Context) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("failed to execute filter: %s, err: %v", executor.postFilters[i].GetName(), err)
				}
			}()
			filter := executor.postFilters[i]
			filter.PostHandle(ctx, result)
		}(ctx)
	}
}
