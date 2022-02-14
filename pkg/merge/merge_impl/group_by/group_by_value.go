//
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

package group_by

import (
	"fmt"
)

import (
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/runtime/xxast"
)

type GroupByValue struct {
	GroupByNode *xxast.GroupByNode
	GroupValues []interface{}
}

func NewGroupByValue(groupByNode *xxast.GroupByNode, row proto.Row) *GroupByValue {
	return &GroupByValue{
		GroupByNode: groupByNode,
		GroupValues: buildGroupValues(groupByNode, row),
	}
}

func buildGroupValues(groupByNode *xxast.GroupByNode, row proto.Row) []interface{} {
	values := make([]interface{}, 0)
	items := groupByNode.Items

	for _, item := range items {
		fmt.Printf("%v", item.Expr())
		// todo age function used to facilitate the test, which will be changed back
		value, err := row.GetColumnValue("age")
		if err != nil {
			panic("get column value error:" + err.Error())
		}
		values = append(values, value)
	}
	return values
}

func (g *GroupByValue) equals(row proto.Row) bool {
	values := buildGroupValues(g.GroupByNode, row)
	for k, _ := range values {
		if fmt.Sprintf("%v", values[k]) != fmt.Sprintf("%v", g.GroupValues[k]) {
			return false
		}
	}
	return true
}
