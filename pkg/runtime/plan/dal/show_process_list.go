package dal

import (
	"context"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/plan"
)

type ShowProcessListPlan struct {
	plan.BasePlan
	db string
	Stmt *ast.ShowProcessList
}

func NewShowProcessListPlan(stmt *ast.ShowProcessList) *ShowProcessListPlan {
	return &ShowProcessListPlan{
		Stmt: stmt,
	}
}

func (s *ShowProcessListPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (s *ShowProcessListPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var (
		sb strings.Builder
		indexes []int
		err     error
	)

	ctx, span := plan.Tracer.Start(ctx, "ShowProcessListPlan.ExecIn")
	defer span.End()

	if err = s.Stmt.Restore(ast.RestoreDefault, &sb, &indexes); err != nil {
		return nil, errors.WithStack(err)
	}

	return conn.Query(ctx, s.db, sb.String(), s.ToArgs(indexes)...)
}

func (s *ShowProcessListPlan) SetDatabase(db string) {
	s.db = db
}
