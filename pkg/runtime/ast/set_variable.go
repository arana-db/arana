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

var _ Statement = (*SetStatement)(nil)

type VariablePair struct {
	Name   string
	Value  ExpressionAtom
	Global bool // @@GLOBAL.xxx
	System bool // @@:true, @:false
}

type SetStatement struct {
	Variables []*VariablePair
}

func (d SetStatement) Restore(rf RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("SET ")

	restoreNext := func(next *VariablePair) error {
		if next.System {
			sb.WriteString("@@")
			if next.Global {
				sb.WriteString("GLOBAL.")
			}
		} else {
			sb.WriteByte('@')
		}

		WriteID(sb, next.Name)

		sb.WriteByte('=')
		if err := next.Value.Restore(rf, sb, args); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	if err := restoreNext(d.Variables[0]); err != nil {
		return nil
	}

	for i := 1; i < len(d.Variables); i++ {
		sb.WriteString(", ")
		if err := restoreNext(d.Variables[i]); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (d SetStatement) CntParams() int {
	var cnt int
	for _, next := range d.Variables {
		cnt += next.Value.CntParams()
	}
	return cnt
}

func (d SetStatement) Mode() SQLType {
	return SQLTypeSetVariable
}
