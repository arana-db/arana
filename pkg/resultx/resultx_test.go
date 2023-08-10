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

package resultx

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestEmptyResult(t *testing.T) {
	res := New()
	defer Drain(res)
	assert.Empty(t, res)
	dataset, err := res.Dataset()
	assert.NoError(t, err)
	assert.Empty(t, dataset)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, affected, uint64(0))
	id, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, id, uint64(0))
}

func TestSlimResult(t *testing.T) {
	res := New(WithLastInsertID(1))
	defer Drain(res)
	assert.NotEmpty(t, res)
	dataset, err := res.Dataset()
	assert.NoError(t, err)
	assert.Empty(t, dataset)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, affected, uint64(0))
	id, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, id, uint64(1))
}

func TestFullResult(t *testing.T) {
	res := New(WithDataset(&utDataset{}), WithRowsAffected(1))
	defer Drain(res)
	assert.NotEmpty(t, res)
	dataset, err := res.Dataset()
	assert.NoError(t, err)
	assert.Empty(t, dataset)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, affected, uint64(1))
	id, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, id, uint64(0))
}

func TestDsResult(t *testing.T) {
	res := New(WithDataset(&utDataset{}))
	defer Drain(res)
	assert.NotEmpty(t, res)
	dataset, err := res.Dataset()
	assert.NoError(t, err)
	assert.Empty(t, dataset)
	affected, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, affected, uint64(0))
	id, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, id, uint64(0))
}

type utDataset struct{}

func (cu *utDataset) Close() error {
	return nil
}

func (cu *utDataset) Fields() ([]proto.Field, error) {
	return nil, nil
}

func (cu *utDataset) Next() (proto.Row, error) {
	return nil, nil
}
