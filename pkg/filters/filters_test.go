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

package filter

import (
	"encoding/json"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/proto"
)

const (
	filterFactoryName = "RegistryFilterFactory"
	filterName        = "PreFilter"
)

func TestGetFilterFactory(t *testing.T) {
	RegistryFilterFactory(filterFactoryName, &FilterFactoryTest{})
	factory := GetFilterFactory(filterFactoryName)
	assert.True(t, factory != nil)
	filter, _ := factory.NewFilter(make([]byte, 0))
	assert.Equal(t, filter.GetName(), "PreFilterTest")
}

func TestGetFilter(t *testing.T) {
	RegisterFilter(filterName, &PreFilterTest{})
	filter := GetFilter(filterName)
	assert.True(t, filter != nil)
	assert.Equal(t, filter.GetName(), "PreFilterTest")
}

type FilterFactoryTest struct {
	proto.FilterFactory
}

func (factory *FilterFactoryTest) NewFilter(config json.RawMessage) (proto.Filter, error) {
	result := &PreFilterTest{}
	return result, nil
}

type PreFilterTest struct {
	proto.PreFilter
}

func (filter *PreFilterTest) GetName() string {
	return "PreFilterTest"
}
