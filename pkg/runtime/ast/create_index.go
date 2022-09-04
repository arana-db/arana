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

var (
	_ Statement = (*CreateIndexStatement)(nil)
	_ Restorer  = (*CreateIndexStatement)(nil)
)

// IndexKeyType is the type for index key.
type IndexKeyType int

// Index key types.
const (
	_ IndexKeyType = iota
	IndexKeyTypeUnique
	IndexKeyTypeSpatial
	IndexKeyTypeFullText
)

type CreateIndexStatement struct {
	IndexName string
	Table     TableName
	KeyType   IndexKeyType
	Keys      []*IndexPartSpec
}

func (c *CreateIndexStatement) CntParams() int {
	return 0
}

func (c *CreateIndexStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("CREATE ")

	switch c.KeyType {
	case IndexKeyTypeUnique:
		sb.WriteString("UNIQUE ")
	case IndexKeyTypeSpatial:
		sb.WriteString("SPATIAL ")
	case IndexKeyTypeFullText:
		sb.WriteString("FULLTEXT ")

	}
	sb.WriteString("INDEX ")
	sb.WriteString(c.IndexName)
	if len(c.Table) == 0 {
		return nil
	}
	sb.WriteString(" ON ")
	if err := c.Table.Restore(flag, sb, args); err != nil {
		return err
	}

	sb.WriteString(" (")

	for i, k := range c.Keys {
		if i != 0 {
			sb.WriteString(", ")
		}
		if err := k.Restore(flag, sb, args); err != nil {
			return err
		}
	}
	sb.WriteString(")")
	return nil
}

func (c *CreateIndexStatement) Mode() SQLType {
	return SQLTypeCreateIndex
}
