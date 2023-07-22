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

func TestFuncCastDate(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastDate)
	assert.Equal(t, 1, fn.NumInput())
	type tt struct {
		inFirst string
		want    string
	}
	for _, v := range []tt{
		{"99-12-2", "1999-12-02"},
		{"99-12-20", "1999-12-20"},
		{"5#2?2", "2005-02-02"},
		{"199#2?2", "0199-02-02"},
		{"12.2+29", "2012-02-29"},
		{"22.2+29", "0000-00-00"},
		{"2.15+20", "0000-00-00"},
		{"2002.5+20", "2002-05-20"},
		{"2002.-5+20", "2002-05-20"},
		{"991202", "1999-12-02"},
		{"19991202", "1999-12-02"},
		{"51202", "2005-12-02"},
		{"051202", "2005-12-02"},
		{"1991202", "0199-12-02"},
		{"20051202", "2005-12-02"},
		{"20051234", "0000-00-00"},
		{"20051314", "0000-00-00"},
		{"20050628", "2005-06-28"},
		{"20050631", "0000-00-00"},
		{"20000229", "2000-02-29"},
		{"20000230", "0000-00-00"},
		{"999990224", "0000-00-00"},
	} {
		t.Run(v.want, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(proto.NewValueString(v.inFirst)))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
