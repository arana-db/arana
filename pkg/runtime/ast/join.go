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
	_ paramsCounter = (*JoinNode)(nil)
	_ Restorer      = (*JoinNode)(nil)
)

const (
	_ JoinType = iota
	LeftJoin
	RightJoin
	InnerJoin
)

type JoinType uint8

type JoinNode struct {
	natural bool
	left    *TableSourceNode
	right   *TableSourceNode
	typ     JoinType
	on      ExpressionNode
}

func (jn *JoinNode) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := jn.left.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	if jn.natural {
		sb.WriteString(" NATURAL")
	}

	sb.WriteByte(' ')

	switch jn.typ {
	case LeftJoin:
		sb.WriteString("LEFT")
	case RightJoin:
		sb.WriteString("RIGHT")
	case InnerJoin:
		sb.WriteString("INNER")
	}

	sb.WriteString(" JOIN ")

	if err := jn.right.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	sb.WriteString(" ON ")

	if err := jn.on.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (jn *JoinNode) CntParams() (n int) {
	if pc, ok := jn.left.source.(paramsCounter); ok {
		n += pc.CntParams()
	}
	if pc, ok := jn.right.source.(paramsCounter); ok {
		n += pc.CntParams()
	}
	n += jn.on.CntParams()
	return
}
