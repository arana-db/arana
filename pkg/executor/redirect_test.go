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

func TestNewRedirectExecutorForSingleDB(t *testing.T) {
	dataSources := &config.DataSourceGroup{
		Master: &config.Source{
			Name:   "master",
			Weight: 10,
		},
	}
	conf := createExecutor(proto.SingleDB, []*config.DataSourceGroup{dataSources})
	redirect, ok := NewRedirectExecutor(conf).(*RedirectExecutor)
	assert.True(t, ok)
	assert.Equal(t, redirect.mode, proto.SingleDB)
	assert.Equal(t, len(redirect.dataSources), 1)
	assert.Equal(t, redirect.dataSources[0].Master.Name, "master")
	assert.Equal(t, redirect.dataSources[0].Master.Weight, 10)
}

func TestNewRedirectExecutorForReadWriteSplitting(t *testing.T) {
	dataSources := &config.DataSourceGroup{
		Master: &config.Source{
			Name:   "master",
			Weight: 10,
		},
		Slaves: []*config.Source{{
			Name:   "slave_a",
			Weight: 5,
		}, {
			Name:   "slave_b",
			Weight: 5,
		}},
	}
	conf := createExecutor(proto.ReadWriteSplitting, []*config.DataSourceGroup{dataSources})
	redirect, ok := NewRedirectExecutor(conf).(*RedirectExecutor)
	assert.True(t, ok)
	assert.Equal(t, redirect.mode, proto.ReadWriteSplitting)
	assert.Equal(t, len(redirect.dataSources), 1)
	assert.Equal(t, redirect.dataSources[0].Slaves[0].Name, "slave_a")
	assert.Equal(t, redirect.dataSources[0].Slaves[0].Weight, 5)
	assert.Equal(t, redirect.dataSources[0].Slaves[1].Name, "slave_b")
	assert.Equal(t, redirect.dataSources[0].Slaves[1].Weight, 5)
	assert.True(t, redirect.dbSelector != nil)
	assert.True(t, redirect.dbSelector.GetDataSourceNo() >= 0)
}

func TestAddPreFilter(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	redirect.AddPreFilter(&PreFilterTest{})
	assert.Equal(t, len(redirect.preFilters), 1)
	assert.Equal(t, redirect.preFilters[0].GetName(), "PreFilterTest")
}

func TestAddPostFilter(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	redirect.AddPostFilter(&PostFilterTest{})
	assert.Equal(t, len(redirect.postFilters), 1)
	assert.Equal(t, redirect.postFilters[0].GetName(), "PostFilterTest")
}

func TestGetPreFilters(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	redirect.AddPreFilter(&PreFilterTest{})
	assert.Equal(t, len(redirect.GetPreFilters()), 1)
	assert.Equal(t, redirect.GetPreFilters()[0].GetName(), "PreFilterTest")
}

func TestGetPostFilters(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	redirect.AddPostFilter(&PostFilterTest{})
	assert.Equal(t, len(redirect.GetPostFilters()), 1)
	assert.Equal(t, redirect.GetPostFilters()[0].GetName(), "PostFilterTest")
}

func TestExecuteMode(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	assert.Equal(t, redirect.ExecuteMode(), proto.SingleDB)
}

func TestProcessDistributedTransaction(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	assert.Equal(t, redirect.ProcessDistributedTransaction(), false)
}

func TestInGlobalTransaction(t *testing.T) {
	conf := createExecutor(proto.SingleDB, make([]*config.DataSourceGroup, 0))
	redirect, _ := NewRedirectExecutor(conf).(*RedirectExecutor)
	assert.Equal(t, redirect.InGlobalTransaction(nil), false)
}

func createExecutor(mode proto.ExecuteMode, dataSources []*config.DataSourceGroup) *config.Executor {
	result := &config.Executor{
		Name:                          "arana",
		Mode:                          mode,
		DataSources:                   dataSources,
		Filters:                       make([]string, 0),
		ProcessDistributedTransaction: true,
	}
	return result
}

type PreFilterTest struct {
	proto.PreFilter
}

func (filter *PreFilterTest) GetName() string {
	return "PreFilterTest"
}

type PostFilterTest struct {
	proto.PostFilter
}

func (filter *PostFilterTest) GetName() string {
	return "PostFilterTest"
}
