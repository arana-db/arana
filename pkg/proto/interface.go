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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

import (
	"github.com/pkg/errors"
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
	}

	Listener interface {
		SetExecutor(executor Executor)

		Listen()

		Close()
	}

	Filter interface {
		GetName() string
	}

	// PreFilter
	PreFilter interface {
		Filter
		PreHandle(ctx *Context)
	}

	// PostFilter
	PostFilter interface {
		Filter
		PostHandle(ctx *Context, result Result)
	}

	FilterFactory interface {
		NewFilter(config json.RawMessage) (Filter, error)
	}

	// Executor
	Executor interface {
		AddPreFilter(filter PreFilter)

		AddPostFilter(filter PostFilter)

		GetPreFilters() []PreFilter

		GetPostFilters() []PostFilter

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

const (
	SingleDB ExecuteMode = iota
	ReadWriteSplitting
	Sharding
)

func (m *ExecuteMode) UnmarshalText(text []byte) error {
	if m == nil {
		return errors.New("can't unmarshal a nil *ExecuteMode")
	}
	if !m.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized execute mode: %q", text)
	}
	return nil
}

func (m *ExecuteMode) unmarshalText(text []byte) bool {
	switch string(text) {
	case "singledb":
		*m = SingleDB
	case "readwritesplitting":
		*m = ReadWriteSplitting
	case "sharding":
		*m = Sharding
	default:
		return false
	}
	return true
}
