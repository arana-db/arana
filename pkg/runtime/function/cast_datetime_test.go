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

func TestFuncCastDatetime(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastDatetime)
	assert.Equal(t, 1, fn.NumInput())
	type tt struct {
		inFirst string
		want    string
	}
	for _, v := range []tt{
		{"99-12-2 1:2:3", "1999-12-02 01:02:03"},
		{"99-12-20T11:2:33", "1999-12-20 11:02:33"},
		{"5#2?2 17%33!24.486762", "2005-02-02 17:33:24"},
		{"199#2?2T23#16+44", "0199-02-02 23:16:44"},
		{"12=2+29 23=59+59.587425", "2012-03-01 00:00:00"},
		{"22=2+29 8=42+11", "0000-00-00 00:00:00"},
		{"2=15+20 11=29+56 ", "0000-00-00 00:00:00"},
		{"2002=5+20 12+34=59.986345", "2002-05-20 12:35:00"},
		{"2002=-5+20 7=32+11", "2002-05-20 07:32:11"},
		{"991202052317.342167", "1999-12-02 05:23:17"},
		{"19991202225959.734128", "1999-12-02 23:00:00"},
		{"51202000000", "2005-12-02 00:00:00"},
		{"051202122458", "2005-12-02 12:24:58"},
		{"1991202091245", "0199-12-02 09:12:45"},
		{"20051202123459.172124", "2005-12-02 12:34:59"},
		{"20051234193247", "0000-00-00 00:00:00"},
		{"20051234561324", "0000-00-00 00:00:00"},
		{"00000000000000", "0000-00-00 00:00:00"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(proto.NewValueString(v.inFirst)))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
