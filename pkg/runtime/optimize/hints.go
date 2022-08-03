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

package optimize

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto/hint"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

func init() {
	RegisterHint(hint.TypeDirect, &Direct{})
	RegisterHint(hint.TypeRoute, &CustomRoute{})

}

type HintExecutor interface {
	exec(tableName ast.TableName, rule *rule.Rule, hints []*hint.Hint) (hintTables rule.DatabaseTables, err error)
}

var (
	hintHandlers = make(map[hint.Type]HintExecutor)
)

// RegisterHint register hint implementation.
func RegisterHint(t hint.Type, h HintExecutor) {
	hintHandlers[t] = h
}

// validate hints of the same type are allowed only once,such as (master||slave) || (router||fullScan||direct)
func validate(hints []*hint.Hint) error {
	var shardingType, nodeType hint.Type

	for _, v := range hints {
		if v.Type == hint.TypeFullScan || v.Type == hint.TypeDirect || v.Type == hint.TypeRoute {
			if shardingType > 0 {
				return errors.Errorf("hint type conflict:%s,%s", shardingType.String(), v.Type.String())
			}
			shardingType = v.Type
		}
		if v.Type == hint.TypeMaster || v.Type == hint.TypeSlave {
			if nodeType > 0 {
				return errors.Errorf("hint type conflict:%s,%s", nodeType.String(), v.Type.String())
			}
			nodeType = v.Type
		}
		//validate TypeRoute
		if v.Type == hint.TypeRoute {
			for _, i := range v.Inputs {
				tb := strings.Split(i.V, ".")
				if len(tb) != 2 {
					return errors.Errorf("route hint format error")
				}
			}
		}

	}
	return nil
}

func Hints(tableName ast.TableName, hints []*hint.Hint, rule *rule.Rule) (hintTables rule.DatabaseTables, err error) {
	if err = validate(hints); err != nil {
		return
	}
	var (
		executor HintExecutor
		ok       bool
	)
	for _, v := range hints {
		if executor, ok = hintHandlers[v.Type]; ok {
			break
		}
	}
	if executor == nil {
		return
	}
	if hintTables, err = executor.exec(tableName, rule, hints); err != nil {
		return
	}
	return
}

// Direct force forward to db[0]
type Direct struct {
}

func (h *Direct) exec(tableName ast.TableName, rule *rule.Rule, hints []*hint.Hint) (hintTables rule.DatabaseTables, err error) {
	db0, _, ok := rule.MustVTable(tableName.Suffix()).Topology().Smallest()
	if !ok {
		return nil, errors.New("not found db0")
	}
	hintTables = make(map[string][]string, 1)
	hintTables[db0] = []string{tableName.String()}
	return hintTables, nil
}

type CustomRoute struct {
}

func (c *CustomRoute) exec(tableName ast.TableName, r *rule.Rule, hints []*hint.Hint) (hintTables rule.DatabaseTables, err error) {
	hintTables = make(map[string][]string)
	for _, h := range hints {
		if h.Type != hint.TypeRoute {
			continue
		}
		for _, i := range h.Inputs {
			tb := strings.Split(i.V, ".")
			hintTables[tb[0]] = append(hintTables[tb[0]], tb[1])
		}
	}
	return
}
