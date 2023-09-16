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
	"github.com/arana-db/arana/pkg/proto"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/testdata"
)

func getGroup(i int) string {
	return fmt.Sprintf("employees_%04d", i)
}

func TestRegister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		tenant = "fakeTenant"
		name   = "employees"
	)

	getDB := func(i int) proto.DB {
		db := testdata.NewMockDB(ctrl)
		db.EXPECT().ID().Return(fmt.Sprintf("the-mysql-instance-%d", i)).AnyTimes()
		db.EXPECT().Weight().Return(proto.Weight{R: 10, W: 10}).AnyTimes()
		db.EXPECT().SetWeight(proto.Weight{R: 9, W: 1}).Return(nil).AnyTimes()
		db.EXPECT().Close().AnyTimes()
		return db
	}

	ns, err := New(name, UpsertDB(getGroup(0), getDB(1)))
	assert.NoError(t, err, "should new namespace ok")
	err = Register(tenant, ns)
	assert.NoError(t, err, "should register namespace ok")

	defer func() {
		err := Unregister(tenant, name)
		assert.NoError(t, err, "should unregister ok")
	}()

	ns = Load(tenant, name)
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

	err = ns.EnqueueCommand(UpdateWeight(getGroup(1), getDB(2).ID(), proto.Weight{R: 9, W: 1}))
	assert.NoError(t, err)
	time.Sleep(5 * time.Millisecond)

	dbs := ns.DBs(getGroup(0))
	assert.NotNil(t, dbs)
	db0 := ns.DB0(context.Background())
	assert.NotNil(t, db0)
	assert.Equal(t, dbs[0].ID(), db0.ID())

	dbm := ns.DBMaster(context.Background(), getGroup(0))
	assert.NotNil(t, dbm)
	dbl := ns.DBSlave(context.Background(), getGroup(0))
	assert.Nil(t, dbl)

	sys := ns.SysDB()
	assert.Nil(t, sys)
	rule := ns.Rule()
	assert.NotNil(t, rule)

	err = ns.EnqueueCommand(RemoveNode(getGroup(0), getDB(1).ID()))
	assert.Nil(t, err)
	time.Sleep(5 * time.Millisecond)

	err = ns.EnqueueCommand(RemoveDB(getGroup(1), getDB(2).ID()))
	assert.Nil(t, err)
	time.Sleep(5 * time.Millisecond)
	db = ns.DB(context.Background(), getGroup(1))
	assert.Nil(t, db)
	err = ns.EnqueueCommand(RemoveGroup(getGroup(1)))
	assert.Nil(t, err)
	time.Sleep(5 * time.Millisecond)
	gp := ns.DBGroups()
	assert.Equal(t, 1, len(gp))
}

func TestGetDBByWeight(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const (
		tenant = "fakeTenant"
		name   = "account"
	)
	// params w: write weight of the db, r: read weight of the db
	getDB := func(i int, w, r int32) proto.DB {
		db := testdata.NewMockDB(ctrl)
		db.EXPECT().ID().Return(fmt.Sprintf("the-mysql-instance-%d", i)).AnyTimes()
		db.EXPECT().Weight().Return(proto.Weight{R: r, W: w}).AnyTimes()
		db.EXPECT().Close().Times(1)
		return db
	}
	// when doing read operation, db 3 is the max
	// when doing write operation, db 2 is the max
	ns, err := New(name,
		UpsertDB(getGroup(0), getDB(1, 9, 1)),
		UpsertDB(getGroup(0), getDB(2, 10, 5)),
		UpsertDB(getGroup(0), getDB(3, 3, 10)),
	)
	assert.NoError(t, err, "should new namespace ok")
	err = Register(tenant, ns)
	assert.NoError(t, err, "should register namespace ok")
	defer func() {
		err := Unregister(tenant, name)
		assert.NoError(t, err, "should unregister ok")
	}()
	time.Sleep(5 * time.Millisecond)
	nsList := List()
	assert.NotNil(t, nsList, "should list namespace")
	assert.Equal(t, 1, len(nsList))
	ns = Load(tenant, name)
	assert.NotNil(t, ns, "should load namespace")
	ctx := rcontext.WithRead(context.Background())
	assert.NotNil(t, ns.DB(ctx, getGroup(0)))
	ctx = rcontext.WithWrite(context.Background())
	assert.NotNil(t, ns.DB(ctx, getGroup(0)))
}
