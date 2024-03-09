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

package ddl

import (
	"context"
	"fmt"
	"strings"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/ddl"
	"github.com/arana-db/arana/pkg/runtime/plan/dml"
	"github.com/arana-db/arana/pkg/util/log"
	parse "github.com/arana-db/parser/ast"
)

func init() {
	optimize.Register(ast.SQLTypeCreateTable, optimizeCreateTable)
}

func optimizeCreateTable(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.CreateTableStmt)

	var (
		shards   rule.DatabaseTables
		fullScan bool
	)
	vt, ok := o.Rule.VTable(stmt.Table.Suffix())
	fullScan = ok
	if !fullScan {
		if stmt.Partition != nil && stmt.Partition.PartitionFrom == 1 {
			vt, ok = drdsCreateTable(ctx, o)
			fullScan = ok
		}
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	toSingle := func(db, tbl string) (proto.Plan, error) {
		ret := &ddl.CreateTablePlan{
			Stmt:     stmt,
			Database: db,
			Tables:   []string{tbl},
		}
		ret.BindArgs(o.Args)

		return ret, nil
	}

	// Go through first table if not full scan.
	if !fullScan {
		return toSingle("", stmt.Table.Suffix())
	}

	// expand all shards if all shards matched
	shards = vt.Topology().Enumerate()

	plans := make([]proto.Plan, 0, len(shards))
	for k, v := range shards {
		next := &ddl.CreateTablePlan{
			Database: k,
			Tables:   v,
			Stmt:     stmt,
		}
		next.BindArgs(o.Args)
		plans = append(plans, next)
	}

	tmpPlan := &dml.CompositePlan{
		Plans: plans,
	}

	return tmpPlan, nil
}

func drdsCreateTable(ctx context.Context, o *optimize.Optimizer) (*rule.VTable, bool) {
	stmt := o.Stmt.(*ast.CreateTableStmt)

	dbName := ctx.Value(proto.ContextKeySchema{}).(string)
	tbName := stmt.Table.Suffix()

	dbColume := ""
	switch node := stmt.Partition.PartitionMethod.Expr.(type) {
	case *parse.ColumnNameExpr:
		dbColume = parse.ColumnNameExpr(*node).Name.Name.O
	}
	tbColume := ""
	switch node := stmt.Partition.Sub.Expr.(type) {
	case *parse.ColumnNameExpr:
		tbColume = parse.ColumnNameExpr(*node).Name.Name.O
	}
	if strings.Compare(dbColume, "") == 0 || strings.Compare(tbColume, "") == 0 {
		return nil, false
	}

	newTb := &config.Table{
		Name: dbName + "." + tbName,
		Sequence: &config.Sequence{
			Type: "snowflake",
		},
		DbRules: []*config.Rule{
			{
				Columns: []*config.ColumnRule{{Name: dbColume}},
				Expr: strings.ReplaceAll(dbColume, dbColume, "$0") + " % " +
					fmt.Sprintf("%d", stmt.Partition.Num*stmt.Partition.Sub.Num) + " / " +
					fmt.Sprintf("%d", stmt.Partition.Sub.Num),
			},
		},
		TblRules: []*config.Rule{
			{
				Columns: []*config.ColumnRule{{Name: tbColume}},
				Expr: strings.ReplaceAll(tbColume, tbColume, "$0") + " % " +
					fmt.Sprintf("%d", stmt.Partition.Num*stmt.Partition.Sub.Num),
			},
		},
		Topology: &config.Topology{
			DbPattern:  dbName + fmt.Sprintf("_${0000..%04d}", stmt.Partition.Num-1),
			TblPattern: tbName + fmt.Sprintf("_${0000..%04d}", stmt.Partition.Num*stmt.Partition.Sub.Num-1),
		},
		Attributes: map[string]string{
			"allow_full_scan": "true",
			"sqlMaxLimit":     "-1",
		},
	}
	newVt, err := config.MakeVTable(tbName, newTb)
	if err == nil {
		o.Rule.SetVTable(tbName, newVt)

		tenant := ctx.Value(proto.ContextKeyTenant{}).(string)
		op, _ := config.NewTenantOperator(config.GetStoreOperate())
		srv := &admin.MyConfigService{
			TenantOp: op,
		}
		tbDTO := &admin.TableDTO{
			Name:           newTb.Name,
			Sequence:       newTb.Sequence,
			DbRules:        newTb.DbRules,
			TblRules:       newTb.TblRules,
			Topology:       newTb.Topology,
			ShadowTopology: newTb.ShadowTopology,
			Attributes:     newTb.Attributes,
		}
		e := srv.UpsertTable(ctx, tenant, dbName, tbName, tbDTO)
		if e == nil {
			return newVt, true
		}
	}

	return nil, false
}
