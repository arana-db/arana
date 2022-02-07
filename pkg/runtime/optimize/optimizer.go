// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package optimize

import (
	"context"
	stdErrors "errors"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/proto/rule"
	"github.com/dubbogo/arana/pkg/runtime/plan"
	"github.com/dubbogo/arana/pkg/runtime/xxast"
	"github.com/dubbogo/arana/pkg/runtime/xxcontext"
	"github.com/dubbogo/arana/pkg/util/log"
)

var _ proto.Optimizer = (*optimizer)(nil)

// errors group
var (
	errNoRuleFound  = stdErrors.New("no rule found")
	errDenyFullScan = stdErrors.New("the full-scan query is not allowed")
)

// IsNoRuleFoundErr returns true if target error is caused by NO-RULE-FOUND.
func IsNoRuleFoundErr(err error) bool {
	return errors.Is(err, errNoRuleFound)
}

// IsDenyFullScanErr returns true if target error is caused by DENY-FULL-SCAN.
func IsDenyFullScanErr(err error) bool {
	return errors.Is(err, errDenyFullScan)
}

type optimizer struct {
}

func (o optimizer) Optimize(ctx context.Context, sql string, args ...interface{}) (proto.Plan, error) {
	stmt, err := xxast.Parse(sql)
	if err != nil {
		return nil, errors.Wrap(err, "optimize failed")
	}

	switch t := stmt.(type) {
	case *xxast.SelectStatement:
		return o.optimizeSelect(ctx, t, args)
	case *xxast.InsertStatement:
	case *xxast.DeleteStatement:
	case *xxast.UpdateStatement:
	}

	//TODO implement all statements
	panic("implement me")
}

func (o optimizer) optimizeSelect(ctx context.Context, stmt *xxast.SelectStatement, args []interface{}) (proto.Plan, error) {
	var ru *rule.Rule
	if ru = xxcontext.Rule(ctx); ru == nil {
		return nil, errors.WithStack(errNoRuleFound)
	}

	shards, fullScan, err := (*Sharder)(ru).Shard(stmt.From[0].TableName(), stmt.Where, args...)
	if err != nil {
		return nil, errors.Wrap(err, "calculate shards failed")
	}

	log.Debugf("compute shards: result=%s, isFullScan=%v", shards, fullScan)

	if fullScan {
		return nil, errors.WithStack(err)
	}

	if len(shards) == 1 {
		for k, v := range shards {
			return &plan.SimpleQueryPlan{
				Database: k,
				Tables:   v,
				Stmt:     stmt,
				Args:     args,
			}, nil
		}
	}

	plans := make([]proto.Plan, 0, len(shards))
	for k, v := range shards {
		plans = append(plans, &plan.SimpleQueryPlan{
			Database: k,
			Tables:   v,
			Stmt:     stmt,
			Args:     args,
		})
	}

	unionPlan := &plan.UnionPlan{
		Plans: plans,
	}
	// TODO: order/groupBy/aggregate

	return unionPlan, nil
}
