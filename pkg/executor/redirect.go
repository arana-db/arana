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

package executor

import (
	"bytes"
	stdErrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"
)

import (
	"github.com/arana-db/parser"
	"github.com/arana-db/parser/ast"
	pMysql "github.com/arana-db/parser/mysql"

	"github.com/pkg/errors"

	rtrace "go.opentelemetry.io/otel/trace"

	"go.uber.org/zap"

	"golang.org/x/exp/slices"
)

import (
	mConstants "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/metrics"
	mysqlErrors "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/transaction"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/trace"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	errMissingTx          = stdErrors.New("no transaction found")
	errNoDatabaseSelected = mysqlErrors.NewSQLError(mConstants.ERNoDb, mConstants.SSNoDatabaseSelected, "No database selected")
	errEmptyQuery         = mysqlErrors.NewSQLError(mConstants.EREmptyQuery, mConstants.SS42000, "Query was empty")
)

var (
	_charsetIndex     map[uint8]string
	_charsetIndexSync sync.Once
)

func getCharsetCollation(c uint8) (string, string) {
	_charsetIndexSync.Do(func() {
		_charsetIndex = make(map[uint8]string)
		for k, v := range pMysql.CharsetIDs {
			k, v := k, v
			_charsetIndex[v] = k
		}
	})
	charset := _charsetIndex[c]
	collation := pMysql.Charsets[charset]
	return charset, collation
}

// IsErrMissingTx returns true if target error was caused by missing-tx.
func IsErrMissingTx(err error) bool {
	return errors.Is(err, errMissingTx)
}

type RedirectExecutor struct {
	localTransactionMap sync.Map // map[uint32]proto.Tx, (connectionID,Tx)
}

func NewRedirectExecutor() *RedirectExecutor {
	return &RedirectExecutor{}
}

func (executor *RedirectExecutor) ProcessDistributedTransaction() bool {
	return false
}

func (executor *RedirectExecutor) InLocalTransaction(ctx *proto.Context) bool {
	_, ok := executor.localTransactionMap.Load(ctx.C.ID())
	return ok
}

func (executor *RedirectExecutor) InGlobalTransaction(ctx *proto.Context) bool {
	return false
}

func (executor *RedirectExecutor) ExecuteUseDB(ctx *proto.Context, db string) error {
	if ctx.C.Schema() == db {
		return nil
	}

	clusters := security.DefaultTenantManager().GetClusters(ctx.C.Tenant())
	if !slices.Contains(clusters, db) {
		return mysqlErrors.NewSQLError(mConstants.ERBadDb, mConstants.SS42000, fmt.Sprintf("Unknown database '%s'", db))
	}

	if hasTx := executor.InLocalTransaction(ctx); hasTx {
		// TODO: should commit existing TX when DB switched
		log.Debugf("commit tx when db switched: conn=%s", ctx.C)
	}

	// bind schema
	ctx.C.SetSchema(db)

	// reset transient variables
	ctx.C.SetTransientVariables(make(map[string]proto.Value))

	return nil
}

func (executor *RedirectExecutor) ExecuteFieldList(ctx *proto.Context) ([]proto.Field, error) {
	index := bytes.IndexByte(ctx.Data, 0x00)
	table := string(ctx.Data[1:index])
	wildcard := string(ctx.Data[index+1:])

	rt, err := runtime.Load(ctx.C.Tenant(), ctx.C.Schema())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if vt, ok := rt.Namespace().Rule().VTable(table); ok {
		if _, atomTable, exist := vt.Topology().Render(0, 0); exist {
			table = atomTable
		}
	}

	db := rt.Namespace().DB0(ctx.Context)
	if db == nil {
		return nil, errors.New("cannot get physical backend connection")
	}

	return db.CallFieldList(ctx.Context, table, wildcard)
}

func (executor *RedirectExecutor) doExecutorComQuery(ctx *proto.Context, act ast.StmtNode) (proto.Result, uint16, error) {
	// switch DB
	switch u := act.(type) {
	case *ast.UseStmt:
		if err := executor.ExecuteUseDB(ctx, u.DBName); err != nil {
			return nil, 0, err
		}
		return resultx.New(), 0, nil
	}

	var (
		start      = time.Now()
		schemaless bool // true if schema is not specified
		err        error
	)
	var hints []*hint.Hint
	for _, next := range act.Hints() {
		var h *hint.Hint
		if h, err = hint.Parse(next); err != nil {
			return nil, 0, err
		}
		hints = append(hints, h)
	}

	// extract trace context
	if trace.Extract(ctx, hints) {
		traceBytes, err := rtrace.SpanFromContext(ctx).SpanContext().MarshalJSON()
		if err != nil {
			return nil, 0, err
		}
		ctx.Context = log.NewContext(ctx.Context, ctx.C.ID(), zap.String("trace-context", strings.ReplaceAll(string(traceBytes), "\"", "")))
	}

	metrics.ParserDuration.Observe(time.Since(start).Seconds())

	if len(ctx.C.Schema()) < 1 {
		// TODO: handle multiple clusters
		clusters := security.DefaultTenantManager().GetClusters(ctx.C.Tenant())
		if len(clusters) != 1 {
			// reject if no schema specified
			return nil, 0, mysqlErrors.NewSQLError(mConstants.ERNoDb, mConstants.SSNoDatabaseSelected, "No database selected")
		}
		schemaless = true
		ctx.C.SetSchema(security.DefaultTenantManager().GetClusters(ctx.C.Tenant())[0])
	}

	ctx.Stmt = &proto.Stmt{
		Hints:    hints,
		StmtNode: act,
	}

	rt, err := runtime.Load(ctx.C.Tenant(), ctx.C.Schema())
	if err != nil {
		return nil, 0, err
	}

	var (
		res  proto.Result
		warn uint16
	)

	switch stmt := act.(type) {
	case *ast.BeginStmt:
		if schemaless {
			err = errNoDatabaseSelected
		} else {
			// begin a new tx
			xaHook, err := transaction.NewXAHook(rcontext.Tenant(ctx), false)
			if err != nil {
				return nil, 0, err
			}
			var tx proto.Tx
			if tx, err = rt.Begin(ctx, xaHook); err == nil {
				executor.putTx(ctx, tx)
				res = resultx.New()
			}
		}
	case *ast.CommitStmt:
		if schemaless {
			err = errNoDatabaseSelected
		} else {
			// remove existing tx, and commit it
			if tx, ok := executor.removeTx(ctx); ok {
				res, warn, err = tx.Commit(ctx.Context)
			} else {
				res, warn, err = nil, 0, errMissingTx
			}
		}
	case *ast.RollbackStmt:
		if schemaless {
			err = errNoDatabaseSelected
		} else {
			// remove existing tx, and rollback it
			if tx, ok := executor.removeTx(ctx); ok {
				res, warn, err = tx.Rollback(ctx.Context)
			} else {
				res, warn, err = nil, 0, errMissingTx
			}
		}
	case *ast.SelectStmt:
		if !schemaless || stmt.From == nil {
			// only SELECT without FROM is allowed in schemaless mode
			// for example: select connection_id()
			if tx, ok := executor.getTx(ctx); ok {
				res, warn, err = tx.Execute(ctx)
			} else {
				res, warn, err = rt.Execute(ctx)
			}
		} else {
			err = errNoDatabaseSelected
		}
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt, *ast.AlterTableStmt:
		if schemaless {
			err = errNoDatabaseSelected
		} else {
			// TODO: merge with other stmt when write-mode is supported for runtime
			if tx, ok := executor.getTx(ctx); ok {
				res, warn, err = tx.Execute(ctx)
			} else {
				res, warn, err = rt.Execute(ctx)
			}
		}
	case *ast.ShowStmt:
		allowSchemaless := func(stmt *ast.ShowStmt) bool {
			switch stmt.Tp {
			case ast.ShowDatabases, ast.ShowVariables, ast.ShowTopology, ast.ShowStatus, ast.ShowTableStatus,
				ast.ShowWarnings, ast.ShowCharset, ast.ShowMasterStatus, ast.ShowProcessList, ast.ShowReplicas, ast.ShowShardingTable,
				ast.ShowReplicaStatus, ast.ShowNodes, ast.ShowUsers, ast.ShowCreateSequence, ast.ShowDatabaseRules:
				return true
			default:
				return false
			}
		}

		if !schemaless || allowSchemaless(stmt) { // only SHOW DATABASES is allowed in schemaless mode
			res, warn, err = rt.Execute(ctx)
		} else {
			err = errNoDatabaseSelected
		}
	case *ast.TruncateTableStmt, *ast.DropTableStmt, *ast.ExplainStmt, *ast.DropIndexStmt, *ast.CreateIndexStmt,
		*ast.AnalyzeTableStmt, *ast.OptimizeTableStmt, *ast.CheckTableStmt, *ast.RenameTableStmt, *ast.RepairTableStmt,
		*ast.CreateTableStmt:
		res, warn, err = executeStmt(ctx, schemaless, rt)
	case *ast.DropTriggerStmt, *ast.SetStmt, *ast.KillStmt:
		res, warn, err = rt.Execute(ctx)
	default:
		if schemaless {
			err = errNoDatabaseSelected
		} else {
			// TODO: mark direct flag temporarily, remove when write-mode is supported for runtime
			ctx.Context = rcontext.WithDirect(ctx.Context)
			if tx, ok := executor.getTx(ctx); ok {
				res, warn, err = tx.Execute(ctx)
			} else {
				res, warn, err = rt.Execute(ctx)
			}
		}
	}

	return res, warn, err
}

func (executor *RedirectExecutor) ExecutorComQuery(ctx *proto.Context, h func(result proto.Result, warns uint16, failure error) error) error {
	p := parser.New()
	query := ctx.GetQuery()

	if len(query) < 1 {
		return h(nil, 0, errEmptyQuery)
	}

	log.DebugfWithLogType(log.LogicalSqlLog, "ComQuery: '%s'", query)

	charset, collation := getCharsetCollation(ctx.C.CharacterSet())

	switch strings.IndexByte(query, ';') {
	case -1: // no ';' exists
		stmt, err := p.ParseOneStmt(query, charset, collation)
		if err != nil {
			return err
		}
		result, warns, failure := executor.doExecutorComQuery(ctx, stmt)
		return h(result, warns, failure)
	case len(query) - 1: // suffix is ';'
		ctx.Data = ctx.Data[:len(ctx.Data)-1]
		stmt, err := p.ParseOneStmt(query[:len(query)-1], charset, collation)
		if err != nil {
			return err
		}
		result, warns, failure := executor.doExecutorComQuery(ctx, stmt)
		return h(result, warns, failure)
	}

	// slow path
	p = parser.New()
	stmts, _, err := p.Parse(query, charset, collation)
	if err != nil {
		return h(nil, 0, err)
	}

	for i := range stmts {
		stmt := stmts[i]
		q := strings.TrimFunc(stmt.OriginalText(), func(r rune) bool {
			return unicode.IsSpace(r) || r == ';'
		})

		ctx2 := *ctx
		ctx2.Data = []byte(q)

		result, warns, failure := executor.doExecutorComQuery(&ctx2, stmt)

		if err := h(result, warns, failure); err != nil {
			return err
		}

		if failure != nil {
			break
		}
	}

	return nil
}

func executeStmt(ctx *proto.Context, schemaless bool, rt runtime.Runtime) (proto.Result, uint16, error) {
	if schemaless {
		return nil, 0, errNoDatabaseSelected
	}
	return rt.Execute(ctx)
}

func (executor *RedirectExecutor) ExecutorComStmtExecute(ctx *proto.Context) (proto.Result, uint16, error) {
	var (
		executable proto.Executable
		err        error
	)

	if tx, ok := executor.getTx(ctx); ok {
		executable = tx
	} else {
		var rt runtime.Runtime
		if rt, err = runtime.Load(ctx.C.Tenant(), ctx.C.Schema()); err != nil {
			return nil, 0, err
		}
		executable = rt
	}

	switch ctx.Stmt.StmtNode.(type) {
	case *ast.SelectStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt, *ast.AlterTableStmt:
	default:
		ctx.Context = rcontext.WithDirect(ctx.Context)
	}

	query := ctx.Stmt.StmtNode.Text()
	log.Debugf("ComStmtExecute: %s", query)

	return executable.Execute(ctx)
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
	//r, ok := executor.localTransactionMap[ctx.connectionID]
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
	ctx.Context = rcontext.WithTransactionID(ctx.Context, tx.ID())
	executor.localTransactionMap.Store(ctx.C.ID(), tx)
}

func (executor *RedirectExecutor) removeTx(ctx *proto.Context) (proto.Tx, bool) {
	exist, ok := executor.localTransactionMap.LoadAndDelete(ctx.C.ID())
	if !ok {
		return nil, false
	}
	ctx.Context = rcontext.WithTransactionID(ctx.Context, exist.(proto.Tx).ID())
	return exist.(proto.Tx), true
}

func (executor *RedirectExecutor) getTx(ctx *proto.Context) (proto.Tx, bool) {
	exist, ok := executor.localTransactionMap.Load(ctx.C.ID())
	if !ok {
		return nil, false
	}
	ctx.Context = rcontext.WithTransactionID(ctx.Context, exist.(proto.Tx).ID())
	return exist.(proto.Tx), true
}
