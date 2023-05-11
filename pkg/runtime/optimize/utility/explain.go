package utility

import (
	"context"
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

	h, err := optimize.GetProcessor(stmt.Target.Mode())
	if err != nil {
		return nil, err
	}
	targetOptimizer := optimize.Optimizer{
		Rule:  o.Rule,
		Hints: o.Hints,
		Stmt:  stmt.Target,
		Args:  o.Args,
	}
	targetPlan, err := h(ctx, &targetOptimizer)
	if err != nil {
		return nil, err
	}
	ret.TargetPlan = targetPlan
	return ret, nil
}
