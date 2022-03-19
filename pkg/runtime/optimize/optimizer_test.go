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
	"testing"
)

import (
	"github.com/dubbogo/parser"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/testdata"
)

func TestOptimizer_OptimizeSelect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	conn := testdata.NewMockVConn(ctrl)

	conn.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, db string, sql string, args ...interface{}) (proto.Result, error) {
			t.Logf("fake query: db=%s, sql=%s, args=%v\n", db, sql, args)
			return nil, nil
		}).
		AnyTimes()

	var (
		sql  = "select * from student where uid in (?,?,?)"
		ctx  = context.Background()
		rule = makeFakeRule(ctrl, 8)
		opt  optimizer
	)

	p := parser.New()
	stmt, _ := p.ParseOneStmt(sql, "", "")

	plan, err := opt.Optimize(rcontext.WithRule(ctx, rule), stmt, 1, 2, 3)
	assert.NoError(t, err)

	_, _ = plan.ExecIn(ctx, conn)
}
