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

package rule

import (
	"fmt"
	"strconv"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
)

var _ rule.ShardComputer = (*exprShardComputer)(nil)

type exprShardComputer struct {
	expr   string
	column string
}

func NewExprShardComputer(expr, column string) (rule.ShardComputer, error) {
	result := &exprShardComputer{
		expr:   expr,
		column: column,
	}
	return result, nil
}

func (compute *exprShardComputer) Compute(value interface{}) (int, error) {
	expr, vars, err := Parse(compute.expr)
	if err != nil {
		return 0, err
	}
	if len(vars) != 1 || vars[0] != Var(compute.column) {
		return 0, errors.Errorf("Parse shard expr is error, expr is: %s", compute.expr)
	}

	shardValue := fmt.Sprintf("%v", value)
	eval, _ := expr.Eval(Env{Var(compute.column): Value(shardValue)})

	result, err := strconv.ParseInt(eval.ToIntString(), 10, 64)
	if err != nil {
		return 0, err
	}

	return int(result), nil
}
