package utility

import (
	"context"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/runtime/plan/dml"
	"github.com/pkg/errors"
	"strings"
)

type ExplainPlan struct {
	plan.BasePlan
	Stmt       *ast.ExplainStatement
	TargetPlan proto.Plan
	DataBase   string
}

func NewExplainPlan(stmt *ast.ExplainStatement) *ExplainPlan {
	return &ExplainPlan{Stmt: stmt}
}

func (e *ExplainPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (e *ExplainPlan) ExecIn(ctx context.Context, vConn proto.VConn) (proto.Result, error) {
	var (
		rf      = ast.RestoreDefault
		sb      strings.Builder
		indexes []int
		res     proto.Result
		err     error
	)
	ctx, span := plan.Tracer.Start(ctx, "DescribePlan.ExecIn")
	defer span.End()

	if err = e.generate(rf, &sb, &indexes); err != nil {
		return nil, errors.Wrap(err, "failed to generate desc/describe sql")
	}

	var (
		query = sb.String()
		args  = e.ToArgs(indexes)
	)

	if res, err = vConn.Query(ctx, e.DataBase, query, args...); err != nil {
		return nil, errors.WithStack(err)
	}

	return res, nil
}

func (e *ExplainPlan) generate(rf ast.RestoreFlag, sb *strings.Builder, args *[]int) error {
	var (
		stmt = *e.Stmt
		err  error
	)
	switch e.TargetPlan.(type) {
	case *dml.SimpleQueryPlan:
		targetPlan := e.TargetPlan.(*dml.SimpleQueryPlan)
		// reset db
		e.resetDataBase(targetPlan.Database)
		// restore explain
		sb.WriteString("EXPLAIN ")
		return targetPlan.Generate(rf, sb, args)
	case *dml.RenamePlan:
		renamePlan := e.TargetPlan.(*dml.RenamePlan)
		switch renamePlan.Plan.(type) {
		case *dml.SimpleQueryPlan:
			targetPlan := renamePlan.Plan.(*dml.SimpleQueryPlan)
			// reset db
			e.resetDataBase(targetPlan.Database)
			// restore explain
			sb.WriteString("EXPLAIN ")
			return targetPlan.Generate(rf, sb, args)
		}
	default:
		// if tabel shards exists, explain stmt restore may not be accurate
		if err = stmt.Restore(ast.RestoreDefault, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (e *ExplainPlan) resetDataBase(db string) {
	e.DataBase = db
}
