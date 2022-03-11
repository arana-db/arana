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

package namespace

import (
	"context"
	"fmt"
	"testing"
	"time"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/testdata"
)

func TestRegister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opt := testdata.NewMockOptimizer(ctrl)

	const (
		name = "employees"
	)

	getGroup := func(i int) string {
		return fmt.Sprintf("employees_%04d", i)
	}
	getDB := func(i int) proto.DB {
		db := testdata.NewMockDB(ctrl)
		db.EXPECT().ID().Return(fmt.Sprintf("the-mysql-instance-%d", i)).AnyTimes()
		db.EXPECT().Weight().Return(proto.Weight{R: 10, W: 10}).AnyTimes()
		db.EXPECT().Close().Times(1)
		return db
	}

	err := Register(New(name, opt, UpsertDB(getGroup(0), getDB(1))))
	assert.NoError(t, err, "should register namespace ok")

	defer func() {
		err := Unregister(name)
		assert.NoError(t, err, "should unregister ok")
	}()

	ns := Load(name)
	assert.NotNil(t, ns, "should load namespace")

	db := ns.DB(context.Background(), getGroup(0))
	assert.NotNil(t, db)
	db = ns.DB(context.Background(), getGroup(1))
	assert.Nil(t, db)

	err = ns.EnqueueCommand(UpsertDB(getGroup(1), getDB(2)))
	assert.NoError(t, err)

	time.Sleep(5 * time.Millisecond)

	db = ns.DB(context.Background(), getGroup(1))
	assert.NotNil(t, db)

	assert.Equal(t, []string{getGroup(0), getGroup(1)}, ns.DBGroups())
}
