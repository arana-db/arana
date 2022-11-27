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

package extvalue

import (
	"github.com/shopspring/decimal"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
)

func Compute(node ast.Node, args []interface{}) (interface{}, error) {
	var vv valueVisitor
	vv.args = args
	ret, err := node.Accept(&vv)
	if err != nil {
		return nil, err
	}

	switch val := ret.(type) {
	case decimal.Decimal:
		if val.IsInteger() {
			return val.IntPart(), nil
		}
		return val.InexactFloat64(), nil
	default:
		return val, nil
	}
}
