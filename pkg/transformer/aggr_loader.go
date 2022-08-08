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
	"github.com/arana-db/arana/pkg/runtime/ast"
	"github.com/arana-db/arana/pkg/runtime/optimize/dml/ext"
)

type AggrLoader struct {
	Aggrs []string
	Alias []string
	Name  []string
}

func LoadAggrs(fields []ast.SelectElement) *AggrLoader {
	aggrLoader := &AggrLoader{
		Alias: make([]string, len(fields)),
	}

	var enter func(n *ast.AggrFunction)
	enter = func(n *ast.AggrFunction) {
		if n == nil {
			return
		}

		aggrLoader.Aggrs = append(aggrLoader.Aggrs, n.Name())
		for _, arg := range n.Args() {
			switch arg.Value.(type) {
			case ast.ColumnNameExpressionAtom:
				columnExpression := arg.Value.(ast.ColumnNameExpressionAtom)
				aggrLoader.Name = append(aggrLoader.Name, columnExpression.String())
			case ast.PredicateExpressionNode:
				enter(arg.Value.(ast.AtomPredicateNode).A.(*ast.FunctionCallExpressionAtom).F.(*ast.AggrFunction))
			}
		}
	}

	for i, field := range fields {
		if field == nil {
			continue
		}

		var aggrFunc *ast.AggrFunction
		switch f := field.(type) {
		case *ext.MappingSelectElement:
			continue
		case ext.SelectElementProvider:
			switch p := f.Prev().(type) {
			case *ast.SelectElementFunction:
				switch af := p.Function().(type) {
				case *ast.AggrFunction:
					aggrFunc = af
				}
			}
		case *ast.SelectElementFunction:
			switch af := f.Function().(type) {
			case *ast.AggrFunction:
				aggrFunc = af
			}
		}

		if aggrFunc != nil {
			aggrLoader.Alias[i] = field.Alias()
			enter(aggrFunc)
		}
	}

	return aggrLoader
}
