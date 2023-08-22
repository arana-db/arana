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

package boot

import (
	"context"
	"os"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/constants"
	_ "github.com/arana-db/arana/pkg/runtime/builtin"
	"github.com/arana-db/arana/testdata"
)

func TestFileProvider(t *testing.T) {
	os.Setenv(constants.EnvConfigPath, testdata.Path("fake_config.yaml"))
	provider := NewDiscovery(testdata.Path("fake_bootstrap.yaml"))

	err := Boot(context.Background(), provider)
	assert.NoError(t, err, "should init ok")

	clusters, err := provider.ListClusters(context.Background(), "arana")
	assert.NoError(t, err)
	assert.NotEmpty(t, clusters, "clusters should not be empty")
	t.Logf("clusters: %v\n", clusters)

	groups, err := provider.ListGroups(context.Background(), "arana", clusters[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, groups, "groups should not be empty")
	t.Logf("groups: %v\n", groups)

	nodes, err := provider.ListNodes(context.Background(), "arana", clusters[0], groups[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, nodes, "nodes should not be empty")

	node, err := provider.GetNode(context.Background(), "arana", clusters[0], groups[0], nodes[0])
	assert.NoError(t, err)
	t.Logf("node: %s\n", node)

	tables, err := provider.ListTables(context.Background(), "arana", clusters[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, tables, "tables should not be empty")
	t.Logf("tables: %v\n", tables)

	table, err := provider.GetTable(context.Background(), "arana", clusters[0], tables[0])
	assert.NoError(t, err)
	assert.True(t, table.AllowFullScan())
	t.Logf("vtable: %v\n", table)

	RunImport(testdata.Path("fake_bootstrap.yaml"), testdata.Path("fake_config.yaml"))

	op, err := config.NewTenantOperator(config.GetStoreOperate())
	assert.NoError(t, err)
	srv := &admin.MyConfigService{
		TenantOp: op,
	}

	var userBody config.User
	userBody.Username = "arana"
	userBody.Password = "654321"
	err = srv.UpsertUser(context.Background(), "arana", &userBody, "arana")
	assert.NoError(t, err)
	err = srv.RemoveUser(context.Background(), "arana", "arana")
	assert.NoError(t, err)

	allGroups, err := srv.ListDBGroups(context.Background(), "arana", "employees")
	assert.NoError(t, err)
	groupBody := allGroups[0]
	groupBody.Nodes = groupBody.Nodes[0:1]
	err = srv.UpsertGroup(context.Background(), "arana", "employees", "employees_0000", groupBody)
	assert.NoError(t, err)
	var groupNew admin.GroupDTO
	groupNew.ClusterName = "employees"
	groupNew.Name = "employees_0100"
	groupNew.Nodes = append(groupNew.Nodes, "node0_r_0")
	err = srv.UpsertGroup(context.Background(), "arana", "employees", "employees_0100", &groupNew)
	assert.NoError(t, err)
	err = srv.RemoveGroup(context.Background(), "arana", "employees", "employees_0100")
	assert.NoError(t, err)

	allNodes, err := srv.ListNodes(context.Background(), "arana")
	assert.NoError(t, err)
	nodeBody := allNodes[0]
	nodeBody.Weight = "r5w10"
	err = srv.UpsertNode(context.Background(), "arana", nodeBody.Name, nodeBody)
	assert.NoError(t, err)
	err = srv.RemoveNode(context.Background(), "arana", "node0_r_0")
	assert.NoError(t, err)

	allTables, err := srv.ListTables(context.Background(), "arana", "employees")
	assert.NoError(t, err)
	tableBody := allTables[0]
	tableBody.Attributes["allow_full_scan"] = "false"
	err = srv.UpsertTable(context.Background(), "arana", "employees", tableBody.Name, tableBody)
	assert.NoError(t, err)
	err = srv.RemoveTable(context.Background(), "arana", "employees", "student")
	assert.NoError(t, err)

	err = srv.RemoveCluster(context.Background(), "arana", "employees")
	assert.NoError(t, err)
	//wait for watcher consumer
	time.Sleep(3 * time.Second)
}
