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

package group

import (
	"fmt"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

type GroupByValue struct {
	groupByColumns []string
	groupByValues  []interface{}
}

func NewGroupByValue(groupByColumns []string, row proto.Row) *GroupByValue {
	return &GroupByValue{
		groupByColumns: groupByColumns,
		groupByValues:  buildGroupValues(groupByColumns, row),
	}
}

func buildGroupValues(groupByColumns []string, row proto.Row) []interface{} {
	values := make([]interface{}, 0)

	for _, column := range groupByColumns {
		value, err := row.(proto.KeyedRow).Get(column)
		if err != nil {
			panic("get column value error:" + err.Error())
		}
		values = append(values, value)
	}
	return values
}

func (g *GroupByValue) equals(row proto.Row) bool {
	values := buildGroupValues(g.groupByColumns, row)
	for k := range values {
		if fmt.Sprintf("%v", values[k]) != fmt.Sprintf("%v", g.groupByValues[k]) {
			return false
		}
	}
	return true
}
