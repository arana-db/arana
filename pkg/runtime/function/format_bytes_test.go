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

package function

import (
	"context"
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestFormatBytes(t *testing.T) {
	fn := proto.MustGetFunc(FuncFormatBytes)
	assert.Equal(t, 1, fn.NumInput())

	out, err := fn.Apply(context.TODO(), proto.ToValuer(nil))
	assert.NoError(t, err)
	assert.Nil(t, out)

	type tt struct {
		input  interface{}
		output string
	}

	for _, next := range []tt{
		{int64(512), "512 bytes"},
		{int64(1025), "1.00 KiB"},
		{3.14, "3 bytes"},
		{-5.55, "-5 bytes"},
		{uint64(18446644073709551615), "16.00 EiB"},
	} {
		t.Run(fmt.Sprint(next.input), func(t *testing.T) {
			val, err := proto.NewValue(next.input)
			assert.NoError(t, err)
			actual, err := fn.Apply(context.TODO(), proto.ToValuer(val))
			assert.NoError(t, err)
			assert.Equal(t, next.output, actual.String())
		})
	}
}

func TestFormatBytes_Nil(t *testing.T) {
	fn := proto.MustGetFunc(FuncFormatBytes)
	assert.Equal(t, 1, fn.NumInput())

	out, err := fn.Apply(context.TODO(), proto.ToValuer(nil))
	assert.NoError(t, err)
	assert.Nil(t, out)

	type tt struct {
		name   string
		input  interface{}
		output string
	}

	for _, next := range []tt{
		{"Test_Nil", nil, "512 bytes"},
	} {
		t.Run(fmt.Sprint(next.name), func(t *testing.T) {
			val, err := proto.NewValue(next.input)
			assert.NoError(t, err)
			actual, err := fn.Apply(context.TODO(), proto.ToValuer(val))
			assert.NoError(t, err)
			assert.Nil(t, actual)
		})
	}
}

func TestFormatBytes_Error(t *testing.T) {
	fn := proto.MustGetFunc(FuncFormatBytes)
	assert.Equal(t, 1, fn.NumInput())

	out, err := fn.Apply(context.TODO(), proto.ToValuer(nil))
	assert.NoError(t, err)
	assert.Nil(t, out)

	type tt struct {
		name   string
		input  interface{}
		output string
	}

	for _, next := range []tt{
		{"Test_Error", "a", "0 bytes"},
		{"Test_Error", "0", "0 bytes"},
	} {
		t.Run(fmt.Sprint(next.name), func(t *testing.T) {
			val, err := proto.NewValue(next.input)
			assert.NoError(t, err)
			actual, err := fn.Apply(context.TODO(), proto.ToValuer(val))
			assert.NoError(t, err)
			assert.Equal(t, next.output, actual.String())
		})
	}
}
