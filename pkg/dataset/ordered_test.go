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

package dataset

import (
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestOrderedDataset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pd := generateFakeParallelDataset(ctrl, 0, 2, 2, 2, 1, 1)
	items := []OrderByItem{
		{
			Column: "id",
			Desc:   false,
		},
		{
			Column: "gender",
			Desc:   true,
		},
	}

	od := NewOrderedDataset(pd, items)
	var pojo fakePojo

	row, err := od.Next()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	t.Logf("next: %#v\n", pojo)
	assert.Equal(t, int64(0), pojo.ID)

	row, err = od.Next()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	t.Logf("next: %#v\n", pojo)
	assert.Equal(t, int64(1), pojo.ID)

	row, err = od.Next()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	t.Logf("next: %#v\n", pojo)
	assert.Equal(t, int64(1), pojo.ID)

	row, err = od.Next()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	t.Logf("next: %#v\n", pojo)
	assert.Equal(t, int64(2), pojo.ID)

	row, err = od.Next()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	t.Logf("next: %#v\n", pojo)
	assert.Equal(t, int64(3), pojo.ID)
}
