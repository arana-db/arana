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

package cmp

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestNewInt64(t *testing.T) {
	c := NewInt64("uid", Cgte, 42)
	assert.Equal(t, "(uid>=42)", c.String())
}

func TestNewDate(t *testing.T) {
	now := time.Now()
	c := NewDate("modified_at", Ceq, now)
	expect := fmt.Sprintf("(modified_at=%s)", now.Format("2006-01-02 15:04:05"))
	assert.Equal(t, expect, c.String())
}

func TestNewString(t *testing.T) {
	c := NewString("name", Ceq, "foobar")
	assert.Equal(t, "(name=foobar)", c.String())
}

func TestNew(t *testing.T) {
	c := New("uid", Ceq, "42", Kint)
	assert.Equal(t, int64(42), c.MustValue())
	assert.Equal(t, Kint, c.Kind())
	assert.Equal(t, "int", c.Kind().String())
	assert.Equal(t, Ceq, c.Comparison())
	assert.Equal(t, "42", c.RawValue())
	assert.Equal(t, "uid", c.Key())

	c.SetKind(Kdate)
	_, err := c.Value()
	assert.Error(t, err, "should parse value failed")
	assert.Equal(t, "(uid=42)", c.String())

	c.v = "1949-10-01"
	_, err = c.Value()
	assert.NoError(t, err)
	assert.NotPanics(t, func() {
		_ = c.MustValue()
	})

	c.SetKind(Kint)
	_, err = c.Value()
	assert.Error(t, err)
	assert.Panics(t, func() {
		_ = c.MustValue()
	})

	c.SetKind(Kstring)
	_, err = c.Value()
	assert.NoError(t, err)
	assert.NotPanics(t, func() {
		_ = c.MustValue()
	})

	c.SetKind(0x7F)
	_, err = c.Value()
	assert.Error(t, err)
}

func TestParseComparison(t *testing.T) {
	type tt struct {
		input  string
		expect Comparison
	}

	for _, it := range []tt{
		{"=", Ceq},
		{">", Cgt},
		{">=", Cgte},
		{"<", Clt},
		{"<=", Clte},
		{"!=", Cne},
		{"<>", Cne},
	} {
		t.Run(it.input, func(t *testing.T) {
			c, ok := ParseComparison(it.input)
			assert.True(t, ok)
			assert.Equal(t, it.expect, c)
		})
	}
}

func TestComparison_WriteTo(t *testing.T) {
	var b bytes.Buffer
	_, err := Ceq.WriteTo(&b)
	assert.NoError(t, err)
	assert.Equal(t, "=", b.String())
}
