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

package ast

const (
	_ UnionType = iota
	UnionTypeAll
	UnionTypeDistinct
)

var (
	_ Statement = (*UnionSelectStatement)(nil)
)

type UnionType uint8

func (u UnionType) String() string {
	switch u {
	case UnionTypeAll:
		return "ALL"
	case UnionTypeDistinct:
		return "DISTINCT"
	default:
		return ""
	}
}

type UnionSelectStatement struct {
	first  *SelectStatement
	others []*UnionStatementItem
}

func (u *UnionSelectStatement) Validate() error {
	if err := u.first.Validate(); err != nil {
		return err
	}

	for _, next := range u.others {
		if err := next.SelectStatement().Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (u *UnionSelectStatement) GetSQLType() SQLType {
	return Squery
}

func (u *UnionSelectStatement) First() *SelectStatement {
	return u.first
}

func (u *UnionSelectStatement) UnionStatementItems() []*UnionStatementItem {
	return u.others
}

type UnionStatementItem struct {
	unionType UnionType
	ss        *SelectStatement
}

func (u UnionStatementItem) Type() UnionType {
	return u.unionType
}

func (u UnionStatementItem) SelectStatement() *SelectStatement {
	return u.ss
}
