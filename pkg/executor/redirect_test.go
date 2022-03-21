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
	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestIsErrMissingTx(t *testing.T) {
	err := errors.WithStack(errMissingTx)
	assert.True(t, IsErrMissingTx(err))
}

func TestAddPreFilter(t *testing.T) {
	redirect := NewRedirectExecutor()
	redirect.AddPreFilter(&PreFilterTest{})
	assert.Equal(t, 1, len(redirect.preFilters))
	assert.Equal(t, "PreFilterTest", redirect.preFilters[0].GetName())
}

func TestAddPostFilter(t *testing.T) {
	redirect := NewRedirectExecutor()
	redirect.AddPostFilter(&PostFilterTest{})
	assert.Equal(t, 1, len(redirect.postFilters))
	assert.Equal(t, "PostFilterTest", redirect.postFilters[0].GetName())
}

func TestGetPreFilters(t *testing.T) {
	redirect := NewRedirectExecutor()
	redirect.AddPreFilter(&PreFilterTest{})
	assert.Equal(t, 1, len(redirect.GetPreFilters()))
	assert.Equal(t, "PreFilterTest", redirect.GetPreFilters()[0].GetName())
}

func TestGetPostFilters(t *testing.T) {
	redirect := NewRedirectExecutor()
	redirect.AddPostFilter(&PostFilterTest{})
	assert.Equal(t, 1, len(redirect.GetPostFilters()))
	assert.Equal(t, "PostFilterTest", redirect.GetPostFilters()[0].GetName())
}

func TestProcessDistributedTransaction(t *testing.T) {
	redirect := NewRedirectExecutor()
	assert.False(t, redirect.ProcessDistributedTransaction())
}

func TestInGlobalTransaction(t *testing.T) {
	redirect := NewRedirectExecutor()
	assert.False(t, redirect.InGlobalTransaction(createContext()))
}

func TestInLocalTransaction(t *testing.T) {
	redirect := NewRedirectExecutor()
	result := redirect.InLocalTransaction(createContext())
	assert.False(t, result)
}

func createContext() *proto.Context {
	result := &proto.Context{
		ConnectionID: 0,
		Data:         make([]byte, 0),
		Stmt:         nil,
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
