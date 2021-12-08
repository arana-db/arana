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
	"fmt"
)

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

import (
	"github.com/dubbogo/kylin/pkg/mysql"
	"github.com/dubbogo/kylin/pkg/proto"
	"github.com/dubbogo/kylin/pkg/resource"
	"github.com/dubbogo/kylin/pkg/util/log"
	"github.com/dubbogo/kylin/third_party/pools"
)

type RedirectExecutor struct {
	localTransactionMap map[uint32]pools.Resource
}

func NewRedirectExecutor() proto.Executor {
	return &RedirectExecutor{
		localTransactionMap: make(map[uint32]pools.Resource, 0),
	}
}

func (executor *RedirectExecutor) AddPreFilter(filter proto.PreFilter) {

}

func (executor *RedirectExecutor) AddPostFilter(filter proto.PostFilter) {

}

func (executor *RedirectExecutor) GetPreFilters() []proto.PreFilter {
	return nil
}

func (executor *RedirectExecutor) GetPostFilter() []proto.PostFilter {
	return nil
}

func (executor *RedirectExecutor) ExecuteMode() proto.ExecuteMode {
	return 0
}

func (executor *RedirectExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *RedirectExecutor) InLocalTransaction(ctx *proto.Context) bool {
	_, ok := executor.localTransactionMap[ctx.ConnectionID]
	return ok
}

func (executor *RedirectExecutor) InGlobalTransaction(ctx *proto.Context) bool {
	return false
}

func (executor *RedirectExecutor) ExecuteUseDB(ctx *proto.Context) error {
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(ctx.MasterDataSource[0])
	r, err := resourcePool.Get(ctx)
	defer func() {
		resourcePool.Put(r)
	}()
	if err != nil {
		return err
	}
	backendConn := r.(*mysql.BackendConnection)
	db := string(ctx.Data[1:])
	return backendConn.WriteComInitDB(db)
}

func (executor *RedirectExecutor) ExecuteFieldList(ctx *proto.Context) ([]proto.Field, error) {
	index := bytes.IndexByte(ctx.Data, 0x00)
	table := string(ctx.Data[0:index])
	wildcard := string(ctx.Data[index+1:])
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(ctx.MasterDataSource[0])
	r, err := resourcePool.Get(ctx)
	defer func() {
		resourcePool.Put(r)
	}()
	if err != nil {
		return nil, err
	}
	backendConn := r.(*mysql.BackendConnection)
	err = backendConn.WriteComFieldList(table, wildcard)
	if err != nil {
		return nil, err
	}

	return backendConn.ReadColumnDefinitions()
}

func (executor *RedirectExecutor) ExecutorComQuery(ctx *proto.Context) (proto.Result, uint16, error) {
	var r pools.Resource
	var err error

	p := parser.New()
	query := string(ctx.Data[1:])
	act, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return nil, 0, err
	}
	log.Debugf("ComQuery: %s", query)

	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(ctx.MasterDataSource[0])
	if _, ok := act.(*ast.BeginStmt); ok {
		r, err = resourcePool.Get(ctx)
		if err != nil {
			return nil, 0, err
		}
		executor.localTransactionMap[ctx.ConnectionID] = r
	} else if _, ok := act.(*ast.CommitStmt); ok {
		r = executor.localTransactionMap[ctx.ConnectionID]
		defer func() {
			resourcePool.Put(r)
		}()
	} else if _, ok := act.(*ast.RollbackStmt); ok {
		r = executor.localTransactionMap[ctx.ConnectionID]
		defer func() {
			resourcePool.Put(r)
		}()
	} else {
		r, err = resourcePool.Get(ctx)
		defer func() {
			resourcePool.Put(r)
		}()
		if err != nil {
			return nil, 0, err
		}
	}

	backendConn := r.(*mysql.BackendConnection)
	result, warn, err := backendConn.ExecuteWithWarningCount(query, 1, true)
	return result, warn, err
}

func (executor *RedirectExecutor) ExecutorComPrepareExecute(ctx *proto.Context) (proto.Result, uint16, error) {
	var r pools.Resource
	var err error
	r, ok := executor.localTransactionMap[ctx.ConnectionID]
	if !ok {
		resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(ctx.MasterDataSource[0])
		r, err = resourcePool.Get(ctx)
		defer func() {
			resourcePool.Put(r)
		}()
		if err != nil {
			return nil, 0, err
		}
	}

	backendConn := r.(*mysql.BackendConnection)
	query, err := generateSql(ctx.Stmt)
	log.Infof(query)
	if err != nil {
		return nil, 0, err
	}
	result, warn, err := backendConn.ExecuteWithWarningCount(query, 1000, true)
	return result, warn, err
}

func (executor *RedirectExecutor) ConnectionClose(ctx *proto.Context) {
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(ctx.MasterDataSource[0])
	r, ok := executor.localTransactionMap[ctx.ConnectionID]
	if ok {
		defer func() {
			resourcePool.Put(r)
		}()
		backendConn := r.(*mysql.BackendConnection)
		_, _, err := backendConn.ExecuteWithWarningCount("rollback", 0, true)
		if err != nil {
			log.Error(err)
		}
	}
}

func generateSql(stmt *proto.Stmt) (string, error) {
	var result []byte
	var j = 0
	sql := []byte(stmt.PrepareStmt)
	for i := 0; i < len(sql); i++ {
		if sql[i] != '?' {
			result = append(result, sql[i])
		} else {
			k := fmt.Sprintf("v%d", j+1)
			quote, val := encodeValue(stmt.BindVars[k])
			if quote {
				val = fmt.Sprintf("'%s'", val)
			}
			result = append(result, []byte(val)...)
			j++
		}
	}
	return string(result), nil
}

// EncodeValue interface to string
func encodeValue(a interface{}) (bool, string) {
	switch a.(type) {
	case nil:
		return false, "NULL"
	case []byte:
		return true, string(a.([]byte))
	default:
		return false, fmt.Sprintf("%v", a)
	}
}
