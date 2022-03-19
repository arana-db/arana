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

package security

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/config"
)

func TestSimpleTenantManager(t *testing.T) {
	var (
		tm = newSimpleTenantManager()
		ok bool
	)

	_, ok = tm.GetTenantOfCluster("fake-cluster")
	assert.False(t, ok)
	_, ok = tm.GetUser("fake-tenant", "fake-user")
	assert.False(t, ok)
	cluster := tm.GetClusters("fake-tenant")
	assert.Empty(t, cluster)

	tm.PutCluster("fake-tenant", "fake-cluster")
	tm.PutUser("fake-tenant", &config.User{
		Username: "fake-user",
	})

	var (
		tenant   string
		user     *config.User
		clusters []string
	)

	tenant, ok = tm.GetTenantOfCluster("fake-cluster")
	assert.True(t, ok)
	assert.Equal(t, "fake-tenant", tenant)

	user, ok = tm.GetUser("fake-tenant", "fake-user")
	assert.True(t, ok)
	assert.NotNil(t, user)
	assert.Equal(t, "fake-user", user.Username)

	clusters = tm.GetClusters("fake-tenant")
	assert.Len(t, clusters, 1)
	assert.Equal(t, []string{"fake-cluster"}, clusters)

	tm.RemoveUser("fake-tenant", "fake-user")
	tm.RemoveCluster("fake-tenant", "fake-cluster")
}
