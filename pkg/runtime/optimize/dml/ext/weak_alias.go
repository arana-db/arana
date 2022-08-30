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

package ext

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/ast"
)

type WeakAliasSelectElement struct {
	ast.SelectElement
	WeakAlias string
}

func (re WeakAliasSelectElement) Prev() ast.SelectElement {
	if p, ok := re.SelectElement.(SelectElementProvider); ok {
		return p.Prev()
	}
	return re.SelectElement
}

func (re WeakAliasSelectElement) Restore(flag ast.RestoreFlag, sb *strings.Builder, args *[]int) error {
	if err := re.SelectElement.Restore(flag, sb, args); err != nil {
		return errors.WithStack(err)
	}
	sb.WriteString(" AS ")
	ast.WriteID(sb, re.WeakAlias)
	return nil
}

func (re WeakAliasSelectElement) Alias() string {
	return re.WeakAlias
}
