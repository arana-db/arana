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

type KillPlan struct {
	plan.BasePlan
	db   string
	Stmt *ast.KillStmt
}

func (k *KillPlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (k *KillPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var sb strings.Builder
	ctx, span := plan.Tracer.Start(ctx, "KillPlan.ExecIn")
	defer span.End()

	if err := k.Stmt.Restore(ast.RestoreDefault, &sb, nil); err != nil {
		return nil, errors.WithStack(err)
	}
	return conn.Exec(ctx, k.db, sb.String())
}

func (k *KillPlan) SetDatabase(db string) {
	k.db = db
}

func NewKillPlan(stmt *ast.KillStmt) *KillPlan {
	return &KillPlan{
		Stmt: stmt,
	}
}
