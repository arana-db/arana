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

package proto

import (
	"context"
)

import (
	"github.com/dubbogo/arana/third_party/pools"
)

type (
	ExecuteMode byte

	// Context
	Context struct {
		context.Context

		ConnectionID uint32

		// sql Data
		Data []byte

		Stmt *Stmt

		MetaDataSource   string
		MasterDataSource []string
		SlaveDataSource  []string
	}

	Listener interface {
		SetExecutor(executor Executor)

		Listen()

		Close()
	}

	// PreFilter
	PreFilter interface {
		PreHandle(ctx Context)
	}

	// PostFilter
	PostFilter interface {
		PostHandle(ctx Context)
	}

	// Executor
	Executor interface {
		AddPreFilter(filter PreFilter)

		AddPostFilter(filter PostFilter)

		GetPreFilters() []PreFilter

		GetPostFilter() []PostFilter

		ExecuteMode() ExecuteMode

		ProcessDistributedTransaction() bool

		InLocalTransaction(ctx *Context) bool

		InGlobalTransaction(ctx *Context) bool

		ExecuteUseDB(ctx *Context) error

		ExecuteFieldList(ctx *Context) ([]Field, error)

		ExecutorComQuery(ctx *Context) (Result, uint16, error)

		ExecutorComPrepareExecute(ctx *Context) (Result, uint16, error)

		ConnectionClose(ctx *Context)
	}

	ResourceManager interface {
		GetMasterResourcePool(name string) *pools.ResourcePool

		GetSlaveResourcePool(name string) *pools.ResourcePool

		GetMetaResourcePool(name string) *pools.ResourcePool
	}
)
