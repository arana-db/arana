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

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

var _ Restorer = (*TableSourceNode)(nil)

type TableSourceNode struct {
	source     interface{} // TableName or Statement or *JoinNode
	alias      string
	partitions []string
	indexHints []*IndexHint
}

func (t *TableSourceNode) ResetTableName(newTableName string) bool {
	switch source := t.source.(type) {
	case TableName:
		var (
			newSource = make(TableName, len(source))
		)
		copy(newSource, source)
		newSource[len(newSource)-1] = newTableName
		t.source = newSource
		return true
	}
	return false
}

func (t *TableSourceNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch source := t.source.(type) {
	case TableName:
		if err := source.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	case *JoinNode:
		if err := source.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	case *SelectStatement:
		sb.WriteByte('(')
		if err := source.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		sb.WriteByte(')')
	case *UnionSelectStatement:
		sb.WriteByte('(')
		if err := source.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
		sb.WriteByte(')')
	default:
		return errors.Errorf("unsupported table source %T!", source)
	}

	if len(t.partitions) > 0 {
		sb.WriteString(" PARTITION (")
		WriteID(sb, t.partitions[0])
		for i := 1; i < len(t.partitions); i++ {
			sb.WriteByte(',')
			WriteID(sb, t.partitions[i])
		}
		sb.WriteByte(')')
	}

	if len(t.alias) > 0 {
		sb.WriteString(" AS ")
		WriteID(sb, t.alias)
	}

	if len(t.indexHints) > 0 {
		sb.WriteByte(' ')
		if err := t.indexHints[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}

		for i := 1; i < len(t.indexHints); i++ {
			sb.WriteString(", ")
			if err := t.indexHints[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func (t *TableSourceNode) Source() interface{} {
	return t.source
}

func (t *TableSourceNode) Partitions() []string {
	return t.partitions
}

func (t *TableSourceNode) IndexHints() []*IndexHint {
	return t.indexHints
}

func (t *TableSourceNode) Alias() string {
	return t.alias
}

func (t *TableSourceNode) TableName() TableName {
	tn, ok := t.source.(TableName)
	if ok {
		return tn
	}
	return nil
}

func (t *TableSourceNode) Join() (*JoinNode, bool) {
	jn, ok := t.source.(*JoinNode)
	return jn, ok
}

func (t *TableSourceNode) SubQuery() Statement {
	stmt, ok := t.source.(Statement)
	if ok {
		return stmt
	}
	return nil
}
