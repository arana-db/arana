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
	"regexp"
	"sync"
)

import (
	"github.com/arana-db/arana/pkg/constants"
)

type ShadowRuleManager interface {
	MatchValueBy(action, column, value string) bool
	MatchHintBy(action, hint string) bool
	MatchRegexBy(action, column, value string) bool
	GetDatabase() string
	GetTableName() string
}

// ShadowRule represents the shadow of databases and tables.
type ShadowRule struct {
	mu    sync.RWMutex
	rules map[string]ShadowRuleManager // map[table]rule
}

func (s *ShadowRule) MatchValueBy(tableName, action, column, value string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rule, ok := s.rules[tableName]
	if !ok {
		return false
	}

	return rule.MatchValueBy(action, column, value)
}

func (s *ShadowRule) MatchHintBy(tableName, action, hint string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rule, ok := s.rules[tableName]
	if !ok {
		return false
	}
	return rule.MatchHintBy(action, hint)
}

func (s *ShadowRule) MatchRegexBy(tableName, action, column, value string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rule, ok := s.rules[tableName]
	if !ok {
		return false
	}
	return rule.MatchRegexBy(action, column, value)
}

func (s *ShadowRule) GetDatabase(database string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return database
}

func (s *ShadowRule) GetTableName(tableName string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return constants.ShadowTablePrefix + tableName
}

func (s *ShadowRule) SetRuleManager(tableName string, ruleManager ShadowRuleManager) {
	s.mu.Lock()
	if s.rules == nil {
		s.rules = make(map[string]ShadowRuleManager, 10)
	}
	s.rules[tableName] = ruleManager
	s.mu.Unlock()
}

func NewShadowRule() *ShadowRule {
	return &ShadowRule{rules: make(map[string]ShadowRuleManager, 0)}
}

type Operation struct {
	enable    bool
	database  string
	tablename string
	actions   map[string][]*Attribute // map[action][]*Attribute, action in (select, update, delete, update)
}

func (o *Operation) GetDatabase() string {
	return o.database
}

func (o *Operation) GetTableName() string {
	return o.tablename
}

func (o *Operation) MatchValueBy(action, column, value string) bool {
	if !o.enable {
		return false
	}
	attrs, ok := o.actions[action]
	if !ok {
		return false
	}

	for _, attr := range attrs {
		if attr.typ == constants.ShadowMatchValue && attr.column == column && attr.value == value {
			return true
		}
	}
	return false
}

func (o *Operation) MatchHintBy(action, hint string) bool {
	if !o.enable {
		return false
	}
	attrs, ok := o.actions[action]
	if !ok {
		return false
	}

	for _, attr := range attrs {
		if attr.typ == constants.ShadowMatchHint {
			return attr.value == hint
		}
	}
	return false
}

// MatchRegexBy .
// TODO impl match regex rule
func (o *Operation) MatchRegexBy(action, column, value string) bool {
	if !o.enable {
		return false
	}
	attrs, ok := o.actions[action]
	if !ok {
		return false
	}
	// TODO impl regex rule below
	for _, attr := range attrs {
		if attr.typ == constants.ShadowMatchRegex {
			reg, err := regexp.Compile(attr.value)
			if err != nil {
				return false
			}
			return reg.MatchString(value)
		}
	}

	return false
}

func NewRuleManager(actions map[string][]*Attribute, enable bool, database string) ShadowRuleManager {
	return &Operation{
		actions:  actions,
		database: database,
		enable:   enable,
	}
}

type Attribute struct {
	column string
	value  string
	typ    string // regex, value, hint
}

func NewAttribute(col, val, typ string) *Attribute {
	return &Attribute{
		column: col,
		typ:    typ,
		value:  val,
	}
}
