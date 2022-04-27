package plan

import (
	"context"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
	"strings"
)

type DropTablePlan struct {
	basePlan
	stmt      *ast.DropTableStatement
	shardsMap []*rule.DatabaseTables
}

func NewDropTablePlan(stmt *ast.DropTableStatement) *DropTablePlan {
	return &DropTablePlan{
		stmt: stmt,
	}
}

func (d DropTablePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (d DropTablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var sb strings.Builder
	var args []int

	for _, shards := range d.shardsMap {
		var stmt = new(ast.DropTableStatement)
		for db, tables := range *shards {
			for _, table := range tables {

				stmt.Tables = append(stmt.Tables, &ast.TableName{
					table,
				})
			}
			err := stmt.Restore(ast.RestoreDefault, &sb, &args)

			if err != nil {
				return nil, err
			}
			_, err = conn.Exec(ctx, db, sb.String(), d.toArgs(args)...)
			if err != nil {
				return nil, err
			}
			sb.Reset()
		}
	}

	return &mysql.Result{AffectedRows: 0}, nil

}

func (s *DropTablePlan) SetShards(shardsMap []*rule.DatabaseTables) {
	s.shardsMap = shardsMap
}
