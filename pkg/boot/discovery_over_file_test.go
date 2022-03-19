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

package boot

import (
	"context"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/testdata"
)

func TestFileProvider(t *testing.T) {
	provider := NewFileProvider(testdata.Path("fake_config.yaml"))

	err := provider.Init(context.Background())
	assert.NoError(t, err, "should init ok")

	clusters, err := provider.ListClusters(context.Background())
	assert.NoError(t, err)
	assert.NotEmpty(t, clusters, "clusters should not be empty")
	t.Logf("clusters: %v\n", clusters)

	groups, err := provider.ListGroups(context.Background(), clusters[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, groups, "groups should not be empty")
	t.Logf("groups: %v\n", groups)

	nodes, err := provider.ListNodes(context.Background(), clusters[0], groups[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, nodes, "nodes should not be empty")

	node, err := provider.GetNode(context.Background(), clusters[0], groups[0], nodes[0])
	assert.NoError(t, err)
	t.Logf("node: %s\n", node)

	tables, err := provider.ListTables(context.Background(), clusters[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, tables, "tables should not be empty")
	t.Logf("tables: %v\n", tables)

	table, err := provider.GetTable(context.Background(), clusters[0], tables[0])
	assert.NoError(t, err)
	assert.True(t, table.AllowFullScan())
	t.Logf("vtable: %v\n", table)
}
