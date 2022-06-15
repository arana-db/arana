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

package schema_manager

import (
	"context"
	"testing"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime"
	"github.com/arana-db/arana/pkg/runtime/namespace"
)

func TestLoader(t *testing.T) {
	t.Skip()
	node := &config.Node{
		Name:      "arana-node-1",
		Host:      "arana-mysql",
		Port:      3306,
		Username:  "root",
		Password:  "123456",
		Database:  "employees",
		ConnProps: nil,
		Weight:    "r10w10",
		Labels:    nil,
	}
	groupName := "employees_0000"
	cmds := make([]namespace.Command, 0)
	cmds = append(cmds, namespace.UpsertDB(groupName, runtime.NewAtomDB(node)))
	namespaceName := "dongjianhui"
	ns := namespace.New(namespaceName, nil, cmds...)
	namespace.Register(ns)
	rt, err := runtime.Load(namespaceName)
	if err != nil {
		panic(err)
	}
	schemeName := "employees"
	tableName := "employees"
	s := NewSimpleSchemaLoader()

	s.Load(context.Background(), rt.(proto.VConn), schemeName, []string{tableName})
}
