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
	_ Node     = (*JoinNode)(nil)
	_ Restorer = (*JoinNode)(nil)
)

const (
	_ JoinType = iota
	LeftJoin
	RightJoin
	InnerJoin
)

var _joinTypeNames = [...]string{
	LeftJoin:  "LEFT",
	RightJoin: "RIGHT",
	InnerJoin: "INNER",
}

type JoinType uint8

func (j JoinType) String() string {
	return _joinTypeNames[j]
}

type JoinNode struct {
	Target  *TableSourceItem
	On      ExpressionNode
	Typ     JoinType
	Natural bool
}

func (jn *JoinNode) Accept(visitor Visitor) (interface{}, error) {
	return visitor.VisitJoin(jn)
}

func (jn *JoinNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if jn.Natural {
		sb.WriteString(" NATURAL")
	}

	sb.WriteByte(' ')
	sb.WriteString(jn.Typ.String())

	sb.WriteString(" JOIN ")

	if err := jn.Target.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	sb.WriteString(" ON ")

	if err := jn.On.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
