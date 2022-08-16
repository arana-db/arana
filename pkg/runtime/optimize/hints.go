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
	RegisterHint(hint.TypeShadow, &Shadow{})
}

type HintExecutor interface {
	exec(tableName ast.TableName, rule *rule.Rule, su *rule.ShadowRule, hints []*hint.Hint) (HintResultLoader, error)
}

type HintResultLoader interface {
	GetShards() rule.DatabaseTables
	GetMatchBy(tableName, action string) bool
}

var hintHandlers = make(map[hint.Type]HintExecutor)

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
		// validate TypeRoute
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

func Hints(tableName ast.TableName, hints []*hint.Hint, rule *rule.Rule, su *rule.ShadowRule) (HintResultLoader, error) {
	if err := validate(hints); err != nil {
		return nil, err
	}
	var (
		executor   HintExecutor
		ok         bool
		hintResult HintResultLoader
		err        error
	)
	for _, v := range hints {
		if executor, ok = hintHandlers[v.Type]; ok {
			break
		}
	}
	if executor == nil {
		return nil, nil
	}
	if hintResult, err = executor.exec(tableName, rule, su, hints); err != nil {
		return nil, err
	}
	return hintResult, nil
}

type Shadow struct{}

func (s *Shadow) exec(tableName ast.TableName, ru *rule.Rule, su *rule.ShadowRule, hints []*hint.Hint) (HintResultLoader, error) {
	shadowLabels := make([]string, 0)
	for _, h := range hints {
		if h.Type != hint.TypeShadow {
			continue
		}
		for _, i := range h.Inputs {
			shadowLabels = append(shadowLabels, i.V)
		}
	}
	return &hintInfo{
		tbls:         make(rule.DatabaseTables, 0),
		shadowLabels: shadowLabels,
		shadowRule:   su,
	}, nil
}

// Direct force forward to db[0]
type Direct struct{}

func (h *Direct) exec(tableName ast.TableName, ru *rule.Rule, su *rule.ShadowRule, hints []*hint.Hint) (HintResultLoader, error) {
	db0, _, ok := ru.MustVTable(tableName.Suffix()).Topology().Smallest()
	if !ok {
		return nil, errors.New("not found db0")
	}
	hintTables := make(rule.DatabaseTables, 1)
	hintTables[db0] = []string{tableName.String()}
	shadowLabels := make([]string, 0)
	for _, ht := range hints {
		if ht.Type != hint.TypeShadow {
			continue
		}
		for _, i := range ht.Inputs {
			shadowLabels = append(shadowLabels, i.V)
		}
	}
	return &hintInfo{
		tbls:         hintTables,
		shadowRule:   su,
		shadowLabels: shadowLabels,
	}, nil
}

type CustomRoute struct{}

func (c *CustomRoute) exec(tableName ast.TableName, ru *rule.Rule, su *rule.ShadowRule, hints []*hint.Hint) (HintResultLoader, error) {
	hintTables := make(rule.DatabaseTables)
	shadowLabels := make([]string, 0)
	for _, h := range hints {
		if h.Type == hint.TypeShadow {
			for _, i := range h.Inputs {
				shadowLabels = append(shadowLabels, i.V)
			}
		}

		if h.Type != hint.TypeRoute {
			continue
		}
		for _, i := range h.Inputs {
			tb := strings.Split(i.V, ".")
			hintTables[tb[0]] = append(hintTables[tb[0]], tb[1])
		}
	}

	return &hintInfo{
		tbls:         hintTables,
		shadowRule:   su,
		shadowLabels: shadowLabels,
	}, nil
}

type hintInfo struct {
	tbls         rule.DatabaseTables
	shadowRule   *rule.ShadowRule
	shadowLabels []string
}

func (h *hintInfo) GetShards() rule.DatabaseTables {
	return h.tbls
}

func (h *hintInfo) GetMatchBy(tableName, action string) bool {
	if h.shadowRule == nil {
		return false
	}
	for _, shadowHint := range h.shadowLabels {
		if h.shadowRule.MatchHintBy(tableName, action, shadowHint) {
			return true
		}
	}
	return false
}
