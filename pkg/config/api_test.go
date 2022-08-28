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

package config_test

import (
	"fmt"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/config"
	_ "github.com/arana-db/arana/pkg/config/etcd"
	_ "github.com/arana-db/arana/pkg/config/file"
	_ "github.com/arana-db/arana/pkg/config/nacos"
	"github.com/arana-db/arana/testdata"
)

func TestInit(t *testing.T) {
	type args struct {
		version string
		options config.Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{"Init_1", args{"file", config.Options{}}, assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, config.Init(tt.args.options, tt.args.version), fmt.Sprintf("Init(%v, %v)", tt.args.options, tt.args.version))
		})
	}
}

func TestRegister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := testdata.NewMockStoreOperate(ctrl)
	mockStore.EXPECT().Name().Times(2).Return("nacos")
	type args struct {
		s config.StoreOperator
	}
	tests := []struct {
		name string
		args args
	}{
		{"Register", args{mockStore}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.Register(tt.args.s)
		})
	}
}

func Test_api(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFileStore := testdata.NewMockStoreOperate(ctrl)
	mockEtcdStore := testdata.NewMockStoreOperate(ctrl)
	mockFileStore.EXPECT().Name().Times(2).Return("file")
	mockEtcdStore.EXPECT().Name().Times(2).Return("etcd")
	config.Register(mockFileStore)
	config.Register(mockEtcdStore)

	mockFileStore2 := testdata.NewMockStoreOperate(ctrl)
	mockFileStore2.EXPECT().Name().AnyTimes().Return("file")
	assert.Panics(t, func() {
		config.Register(mockFileStore2)
	}, "StoreOperate=[file] already exist")
}

func Test_Init(t *testing.T) {
	options := config.Options{
		StoreName: "fake",
		RootPath:  "",
		Options:   nil,
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFileStore := testdata.NewMockStoreOperate(ctrl)
	mockFileStore.EXPECT().Name().Times(2).Return("fake")
	mockFileStore.EXPECT().Init(options).Return(nil)
	err := config.Init(options, "fake")
	assert.Error(t, err)

	config.Register(mockFileStore)
	err = config.Init(options, "fake")
	assert.NoError(t, err)

	store := config.GetStoreOperate()
	assert.NotNil(t, store)
}
