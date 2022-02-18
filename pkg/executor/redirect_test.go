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

package executor

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/config"
	"github.com/dubbogo/arana/pkg/proto"
)

func TestNewRedirectExecutor(t *testing.T) {
	a := 20
	dataSources := &config.DataSourceGroup{
		Master: &config.Source{
			Name:   "master",
			Weight: &a,
		},
		Slaves: []*config.Source{{
			Name:   "slave_a",
			Weight: &a,
		}, {
			Name:   "slave_b",
			Weight: &a,
		}},
	}
	conf := &config.Executor{
		Name:                          "arana",
		Mode:                          proto.SingleDB,
		DataSources:                   []*config.DataSourceGroup{dataSources},
		Filters:                       make([]string, 0),
		ProcessDistributedTransaction: true,
	}
	redirect, ok := NewRedirectExecutor(conf).(*RedirectExecutor)
	assert.True(t, ok)
	assert.Equal(t, redirect.mode, proto.SingleDB)
	assert.Equal(t, len(redirect.dataSources), 1)
	assert.Equal(t, redirect.dataSources[0].Master.Name, "master")
	assert.Equal(t, *redirect.dataSources[0].Master.Weight, 20)
	assert.Equal(t, redirect.dataSources[0].Slaves[0].Name, "slave_a")
	assert.Equal(t, *redirect.dataSources[0].Slaves[0].Weight, 20)
	assert.Equal(t, redirect.dataSources[0].Slaves[1].Name, "slave_b")
	assert.Equal(t, *redirect.dataSources[0].Slaves[1].Weight, 20)
}
