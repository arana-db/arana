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

package ast

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

var (
	_ Restorer = (*TableSourceNode)(nil)
	_ Node     = (*TableSourceNode)(nil)
	_ Restorer = (*TableSourceItem)(nil)
)

type TableSourceItem struct {
	Source     Node // TableName,*SelectStatement,*UnionSelectStatement
	Alias      string
	Partitions []string
	IndexHints []*IndexHint
}

func (t *TableSourceItem) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	switch source := t.Source.(type) {
	case TableName:
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
		panic("unreachable")
	}

	if len(t.Partitions) > 0 {
		sb.WriteString(" PARTITION (")
		WriteID(sb, t.Partitions[0])
		for i := 1; i < len(t.Partitions); i++ {
			sb.WriteByte(',')
			WriteID(sb, t.Partitions[i])
		}
		sb.WriteByte(')')
	}

	if len(t.Alias) > 0 {
		sb.WriteString(" AS ")
		WriteID(sb, t.Alias)
	}

	if len(t.IndexHints) > 0 {
		sb.WriteByte(' ')
		if err := t.IndexHints[0].Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}

		for i := 1; i < len(t.IndexHints); i++ {
			sb.WriteString(", ")
			if err := t.IndexHints[i].Restore(flag, sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

func (t *TableSourceItem) ResetTableName(newTableName string) bool {
	switch source := t.Source.(type) {
	case TableName:
		newSource := make(TableName, len(source))
		copy(newSource, source)
		newSource[len(newSource)-1] = newTableName
		t.Source = newSource
		return true
	}
	return false
}

type TableSourceNode struct {
	TableSourceItem
	Joins []*JoinNode
}

func (t *TableSourceNode) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitTableSource(t)
}

func (t *TableSourceNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := t.TableSourceItem.Restore(flag, sb, args); err != nil {
		return err
	}

	for _, next := range t.Joins {
		if err := next.Restore(flag, sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}
