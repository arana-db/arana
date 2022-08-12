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
	"github.com/arana-db/arana/pkg/runtime/ast"
)

var (
	_ ast.SelectElement     = (*WeakSelectElement)(nil)
	_ SelectElementProvider = (*WeakSelectElement)(nil)

	_ ast.SelectElement     = (*OrderedSelectElement)(nil)
	_ SelectElementProvider = (*OrderedSelectElement)(nil)

	_ ast.SelectElement     = (*MappingSelectElement)(nil)
	_ SelectElementProvider = (*MappingSelectElement)(nil)
)

// WeakSelectElement represents a temporary SelectElement which will be cleaned finally.
type WeakSelectElement struct {
	ast.SelectElement
}

func (w WeakSelectElement) Weak() {
}

func (w WeakSelectElement) Prev() ast.SelectElement {
	if p, ok := w.SelectElement.(SelectElementProvider); ok {
		return p.Prev()
	}
	return w.SelectElement
}

// OrderedSelectElement represents the select element is designed for order-by or group-by.
type OrderedSelectElement struct {
	ast.SelectElement
	Ordinal int
	Desc    bool
	GroupBy bool
	OrderBy bool
}

func (o OrderedSelectElement) Prev() ast.SelectElement {
	if p, ok := o.SelectElement.(SelectElementProvider); ok {
		return p.Prev()
	}
	return o.SelectElement
}

type MappingSelectElement struct {
	ast.SelectElement
	Mapping ast.SelectElement
}

func (vt MappingSelectElement) Prev() ast.SelectElement {
	if p, ok := vt.SelectElement.(SelectElementProvider); ok {
		return p.Prev()
	}
	return vt.SelectElement
}
