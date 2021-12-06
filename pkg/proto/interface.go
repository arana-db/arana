package proto

import (
	"context"
)

import (
	"github.com/dubbogo/kylin/third_party/pools"
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
