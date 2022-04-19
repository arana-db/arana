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

type ShowTablesPlan struct {
	Plans []proto.Plan
}

func (u ShowTablesPlan) Type() proto.PlanType {
	return proto.PlanTypeQuery
}

func (u ShowTablesPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
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
