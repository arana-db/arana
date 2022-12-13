/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dal

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/namespace"
	"github.com/arana-db/arana/pkg/runtime/optimize"
	"github.com/arana-db/arana/pkg/runtime/plan/dal"
	"github.com/arana-db/arana/pkg/util/math"
)

const (
	sep = "_"
)

func init() {
	optimize.Register(ast.SQLTypeKill, optimizeKill)
}

func optimizeKill(ctx context.Context, o *optimize.Optimizer) (proto.Plan, error) {
	stmt := o.Stmt.(*ast.KillStmt)
	ret := dal.NewKillPlan(stmt)

	processId, groupId := math.DecodeProcessID(int64(stmt.ConnectionID), math.DefaultBase)
	stmt.ConnectionID = uint64(processId)

	groups := namespace.Load(rcontext.Schema(ctx)).DBGroups()
	for _, group := range groups {
		strs := strings.Split(group, sep)
		if len(strs) < 2 {
			continue
		}

		t, err := strconv.ParseInt(strs[1], 10, 64)
		if err == nil && t == groupId {
			ret.SetDatabase(group)
			return ret, nil
		}
	}

	return nil, fmt.Errorf("can't find a proper db group")
}
