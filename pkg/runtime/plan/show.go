package plan

import (
	"context"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type ShowPlan struct {
	Plans []proto.Plan
}

func (u ShowPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (u ShowPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	var results []proto.Result
	for _, it := range u.Plans {
		res, err := it.ExecIn(ctx, conn)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		results = append(results, res)
	}
	return compositeResult(results), nil
}
