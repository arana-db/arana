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

package rule

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestAttributes(t *testing.T) {
	const key byte = 0x01
	var (
		attrs attributes
		ok    bool
	)

	_, ok = attrs.attribute(key)
	assert.False(t, ok)
	_, ok = attrs.attributeUint32(key)
	assert.False(t, ok)
	_, ok = attrs.attributeUint64(key)
	assert.False(t, ok)

	attrs.setAttribute(key, []byte("foobar"))
	val, ok := attrs.attribute(key)
	assert.True(t, ok)
	assert.Equal(t, []byte("foobar"), val)

	attrs.setAttributeBool(key, true)
	b, ok := attrs.attributeBool(key)
	assert.True(t, ok)
	assert.True(t, b)

	attrs.setAttributeBool(key, false)
	b, ok = attrs.attributeBool(key)
	assert.True(t, ok)
	assert.False(t, b)

	attrs.setAttributeUint32(key, 1234)
	i, ok := attrs.attributeUint32(key)
	assert.True(t, ok)
	assert.Equal(t, uint32(1234), i)

	attrs.setAttributeUint64(key, 5678)
	l, ok := attrs.attributeUint64(key)
	assert.True(t, ok)
	assert.Equal(t, uint64(5678), l)
}

func TestAttributesNil(t *testing.T) {
	const key byte = 0x02

	var (
		attrs = (*attributes)(nil)
		ok    bool
	)

	_, ok = attrs.attribute(key)
	assert.False(t, ok)
	_, ok = attrs.attributeBool(key)
	assert.False(t, ok)
	_, ok = attrs.attributeUint32(key)
	assert.False(t, ok)
	_, ok = attrs.attributeUint64(key)
	assert.False(t, ok)

	type tt struct {
		attributes
	}

	var c tt

	c.setAttribute(key, []byte("foo"))
	b, ok := c.attribute(key)
	assert.True(t, ok)
	assert.Equal(t, []byte("foo"), b)
}
