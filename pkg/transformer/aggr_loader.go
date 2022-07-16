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

package transformer

import (
	ast2 "github.com/arana-db/arana/pkg/runtime/ast"
)

type AggrLoader struct {
	Aggrs []string
	Alias []string
	Name  []string
}

func LoadAggrs(fields []ast2.SelectElement) *AggrLoader {
	aggrLoader := &AggrLoader{
		Alias: make([]string, len(fields)),
	}

	var enter func(n *ast2.AggrFunction)
	enter = func(n *ast2.AggrFunction) {
		if n == nil {
			return
		}

		aggrLoader.Aggrs = append(aggrLoader.Aggrs, n.Name())
		for _, arg := range n.Args() {
			switch arg.Value().(type) {
			case ast2.ColumnNameExpressionAtom:
				columnExpression := arg.Value().(ast2.ColumnNameExpressionAtom)
				aggrLoader.Name = append(aggrLoader.Name, columnExpression.String())
			case ast2.PredicateExpressionNode:
				enter(arg.Value().(ast2.AtomPredicateNode).A.(*ast2.FunctionCallExpressionAtom).F.(*ast2.AggrFunction))
			}
		}
	}

	for i, field := range fields {
		if field == nil {
			continue
		}
		if f, ok := field.(*ast2.SelectElementFunction); ok {
			aggrLoader.Alias[i] = field.Alias()
			enter(f.Function().(*ast2.AggrFunction))
		}
	}

	return aggrLoader
}
