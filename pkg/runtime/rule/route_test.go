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
package rule_test

import (
	"sort"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/cmp"
	. "github.com/dubbogo/arana/pkg/runtime/rule"
)

func TestRule_Route(t *testing.T) {
	ru := makeTestRule(4)

	const key = "uid"

	tb := []struct {
		C *cmp.Comparative
		E []int
	}{
		// simple
		{cmp.NewInt64(key, cmp.Ceq, 1), []int{1}},
		{cmp.NewInt64(key, cmp.Cgt, 1), nil},
		{cmp.NewInt64(key, cmp.Cgte, 1), nil},
		{cmp.NewInt64(key, cmp.Clt, 3), nil},
		{cmp.NewInt64(key, cmp.Clte, 3), nil},
	}
	for _, it := range tb {
		mat, err := Route(ru, fakeTable, it.C)
		assert.NoError(t, err)
		ret, err := mat.Eval()
		assert.NoError(t, err, "eval failed")
		t.Logf("+++++++++ %s ++++++++\n", it.C)

		actual := make([]int, 0)
		for ret.HasNext() {
			actual = append(actual, int(ret.Next().(int64)))
		}

		t.Log("route result:", actual)
		if it.E != nil {
			sort.Ints(actual)
			sort.Ints(it.E)
			assert.EqualValues(t, it.E, actual)
		}
	}
}
