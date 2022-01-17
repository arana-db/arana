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
	"fmt"
)

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

import (
	"github.com/dubbogo/arana/pkg/config"
	"github.com/dubbogo/arana/pkg/mysql"
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/resource"
	"github.com/dubbogo/arana/pkg/selector"
	"github.com/dubbogo/arana/pkg/util/log"
	"github.com/dubbogo/arana/third_party/pools"
)

type RedirectExecutor struct {
	mode                proto.ExecuteMode
	preFilters          []proto.PreFilter
	postFilters         []proto.PostFilter
	dataSources         []*config.DataSourceGroup
	localTransactionMap map[uint32]pools.Resource
	dbSelector          selector.Selector
}

func NewRedirectExecutor(conf *config.Executor) proto.Executor {
	executor := &RedirectExecutor{
		mode:                conf.Mode,
		preFilters:          make([]proto.PreFilter, 0),
		postFilters:         make([]proto.PostFilter, 0),
		dataSources:         conf.DataSources,
		localTransactionMap: make(map[uint32]pools.Resource, 0),
	}

	if conf.Mode == proto.ReadWriteSplitting && len(conf.DataSources) > 0 {
		weights := make([]int, 0, len(conf.DataSources[0].Slaves))
		for _, v := range conf.DataSources[0].Slaves {
			if v.Weight == nil {
				v.Weight = &selector.DefaultWeight
			}
			weights = append(weights, *v.Weight)
		}
		executor.dbSelector = selector.NewWeightRandomSelector(weights)
	}

	return executor
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

func (executor *RedirectExecutor) ExecuteMode() proto.ExecuteMode {
	return executor.mode
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
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
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
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
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

	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
	switch act.(type) {
	case *ast.BeginStmt:
		r, err = resourcePool.Get(ctx)
		if err != nil {
			return nil, 0, err
		}
		executor.localTransactionMap[ctx.ConnectionID] = r
	case *ast.CommitStmt:
		r = executor.localTransactionMap[ctx.ConnectionID]
		defer func() {
			delete(executor.localTransactionMap, ctx.ConnectionID)
			resourcePool.Put(r)
		}()
	case *ast.RollbackStmt:
		r = executor.localTransactionMap[ctx.ConnectionID]
		defer func() {
			delete(executor.localTransactionMap, ctx.ConnectionID)
			resourcePool.Put(r)
		}()
	case *ast.SelectStmt:
		switch executor.ExecuteMode() {
		case proto.ReadWriteSplitting:
			return executor.slaveComQueryExecute(ctx, query)
		}
	default:
		r, err = resourcePool.Get(ctx)
		defer func() {
			resourcePool.Put(r)
		}()
		if err != nil {
			return nil, 0, err
		}
	}

	backendConn := r.(*mysql.BackendConnection)
	executor.doPreFilter(ctx)
	result, warn, err := backendConn.ExecuteWithWarningCount(query, true)
	executor.doPostFilter(ctx, result)
	return result, warn, err
}

func (executor *RedirectExecutor) ExecutorComStmtExecute(ctx *proto.Context) (proto.Result, uint16, error) {
	var r pools.Resource
	var err error
	r, ok := executor.localTransactionMap[ctx.ConnectionID]
	if !ok {
		resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
		r, err = resourcePool.Get(ctx)
		defer func() {
			resourcePool.Put(r)
		}()
		if err != nil {
			return nil, 0, err
		}
	}

	switch executor.ExecuteMode() {
	case proto.ReadWriteSplitting:
		switch ctx.Stmt.StmtNode.(type) {
		case *ast.SelectStmt:
			return executor.slaveComStmtExecute(ctx)
		}
	}

	backendConn := r.(*mysql.BackendConnection)
	query := ctx.Stmt.StmtNode.Text()
	log.Infof(query)

	executor.doPreFilter(ctx)
	result, warn, err := backendConn.PrepareQuery(query, ctx.Data)
	executor.doPostFilter(ctx, result)
	return result, warn, err
}

func (executor *RedirectExecutor) ConnectionClose(ctx *proto.Context) {
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(executor.dataSources[0].Master.Name)
	r, ok := executor.localTransactionMap[ctx.ConnectionID]
	if ok {
		defer func() {
			resourcePool.Put(r)
		}()
		backendConn := r.(*mysql.BackendConnection)
		_, _, err := backendConn.ExecuteWithWarningCount("rollback", true)
		if err != nil {
			log.Error(err)
		}
	}
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

func (executor *RedirectExecutor) slaveComQueryExecute(ctx *proto.Context, query string) (proto.Result, uint16, error) {

	dsNo := executor.dbSelector.GetDataSourceNo()
	resourcePool := resource.GetDataSourceManager().GetSlaveResourcePool(executor.dataSources[0].Slaves[dsNo].Name)
	r, err := resourcePool.Get(ctx)
	defer func() {
		resourcePool.Put(r)
	}()
	if err != nil {
		return nil, 0, err
	}

	backendConn := r.(*mysql.BackendConnection)

	executor.doPreFilter(ctx)
	result, warn, err := backendConn.ExecuteWithWarningCount(query, true)
	executor.doPostFilter(ctx, result)
	return result, warn, err
}

func (executor *RedirectExecutor) slaveComStmtExecute(ctx *proto.Context) (proto.Result, uint16, error) {
	var (
		r           pools.Resource
		backendConn *mysql.BackendConnection
		err         error
	)
	dsNo := executor.dbSelector.GetDataSourceNo()
	fmt.Println(dsNo)
	resourcePool := resource.GetDataSourceManager().GetSlaveResourcePool(executor.dataSources[0].Slaves[dsNo].Name)
	r, err = resourcePool.Get(ctx)
	defer func() {
		resourcePool.Put(r)
	}()
	if err != nil {
		return nil, 0, err
	}

	backendConn = r.(*mysql.BackendConnection)

	query := ctx.Stmt.StmtNode.Text()
	log.Infof(query)

	executor.doPreFilter(ctx)
	result, warn, err := backendConn.PrepareQuery(query, ctx.Data)
	executor.doPostFilter(ctx, result)
	return result, warn, err
}
