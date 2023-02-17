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

func TestFuncCastTime(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastTime)
	assert.Equal(t, 1, fn.NumInput())
	type tt struct {
		inFirst string
		want    string
	}
	for _, v := range []tt{
		{"1 1:2:3.111111", "25:02:03"},
		{"2 1:2.599999", "49:02:01"},
		{"3 1", "73:00:00"},
		{"-34 22", "-838:00:00"},
		{"35 1", "838:59:59"},
		{"-838:59:59", "-838:59:59"},
		{"1:2:3", "01:02:03"},
		{"1:1", "01:01:00"},
		{"1:100", "00:00:00"},
		{"1:b", "00:00:00"},
		{"838:12:11", "838:12:11"},
		{"839:12:11", "838:59:59"},
		{"1", "00:00:01"},
		{"102", "00:01:02"},
		{"51219", "05:12:19"},
		{"173429", "17:34:29"},
		{"173470", "00:00:00"},
		{"176429", "00:00:00"},
		{"17ab10", "00:00:00"},
		{"8381211", "838:12:11"},
		{"8391211", "838:59:59"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(proto.NewValueString(v.inFirst)))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
