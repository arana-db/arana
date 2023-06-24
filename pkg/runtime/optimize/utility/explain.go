package utility

import (
	"context"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/plan/utility"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize"
)

func init() {
	optimize.Register(ast.SQLTypeExplain, optimzeExplainStatement)
}

func optimzeExplainStatement(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.ExplainStatement)

	ret := utility.NewExplainPlan(stmt)

	var (
		shards rule.DatabaseTables
		err    error
	)

	shards, err = o.ComputeShards(ctx, stmt.Table, nil, o.Args)
	if err != nil {
		return nil, err
	}

	ret.SetShards(shards)

	return ret, nil
}
