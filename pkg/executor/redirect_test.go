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
	"github.com/dubbogo/arana/pkg/proto"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/config"
)

func TestNewRedirectExecutor(t *testing.T) {
	a := 20
	slaveSourceA := &config.Source{
		Name:   "slave_a",
		Weight: &a,
	}
	slaveSourceB := &config.Source{
		Name:   "slave_b",
		Weight: nil,
	}
	slaveSources := []*config.Source{slaveSourceA, slaveSourceB}
	dataSources := &config.DataSourceGroup{
		Master: &config.Source{
			Name:   "master",
			Weight: nil,
		},
		Slaves: slaveSources,
	}
	conf := &config.Executor{
		Name:                          "arana",
		Mode:                          1,
		DataSources:                   []*config.DataSourceGroup{dataSources},
		Filters:                       nil,
		ProcessDistributedTransaction: true,
	}
	executor := NewRedirectExecutor(conf)
	assert.False(t, executor == nil)
}
