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
	"database/sql"
)

import (
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
	return false
}

func (executor *RedirectExecutor) InGlobalTransaction(ctx *proto.Context) bool {
	return false
}

func (executor *RedirectExecutor) ExecuteUseDB(ctx *proto.Context) error {
	return nil
}

func (executor *RedirectExecutor) ExecuteFieldList(ctx *proto.Context) ([]proto.Field, error) {
	return nil, nil
}

func (executor *RedirectExecutor) ExecutorComQuery(ctx *proto.Context) (proto.Result, uint16, error) {
	resourcePool := resource.GetDataSourceManager().GetMasterResourcePool(ctx.MasterDataSource[0])
	r, err := resourcePool.Get(ctx)
	if err != nil {
		return nil, 0, err
	}
	query := string(ctx.Data[1:])
	log.Debugf("ComQuery: %s", query)
	db := r.(*sql.DB)
	row, err := db.Query(query)
	if err != nil {
		return nil, 0, err
	}
	// todo convert row to proto.result
	return nil, 0, nil
}

func (executor *RedirectExecutor) ExecutorComPrepareExecute(ctx *proto.Context) (proto.Result, uint16, error) {
	return nil, 0, nil
}

func (executor *RedirectExecutor) ConnectionClose(ctx *proto.Context) {

}
