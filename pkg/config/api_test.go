package config

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetStoreOperate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	//mockStore := NewMockStoreOperate(ctrl)
	tests := []struct {
		name    string
		want    StoreOperate
		wantErr assert.ErrorAssertionFunc
	}{
		{"GetStoreOperate_1", nil, assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetStoreOperate()
			if !tt.wantErr(t, err, fmt.Sprintf("GetStoreOperate()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetStoreOperate()")
		})
	}
}

func TestInit(t *testing.T) {
	type args struct {
		name    string
		options map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{"Init_1", args{"file", nil}, assert.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, Init(tt.args.name, tt.args.options), fmt.Sprintf("Init(%v, %v)", tt.args.name, tt.args.options))
		})
	}
}

func TestRegister(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockStore := NewMockStoreOperate(ctrl)
	mockStore.EXPECT().Name().Times(2).Return("nacos")
	type args struct {
		s StoreOperate
	}
	tests := []struct {
		name string
		args args
	}{
		{"Register", args{mockStore}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Register(tt.args.s)
		})
	}
}

func Test_api(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFileStore := NewMockStoreOperate(ctrl)
	mockEtcdStore := NewMockStoreOperate(ctrl)
	mockFileStore.EXPECT().Name().Times(2).Return("file")
	mockEtcdStore.EXPECT().Name().Times(2).Return("etcd")
	Register(mockFileStore)
	Register(mockEtcdStore)
	assert.True(t, len(slots) > 0)

	mockFileStore2 := NewMockStoreOperate(ctrl)
	mockFileStore2.EXPECT().Name().AnyTimes().Return("file")
	assert.Panics(t, func() {
		Register(mockFileStore2)
	}, "StoreOperate=[file] already exist")
}

func Test_Init(t *testing.T) {
	options := make(map[string]interface{}, 0)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFileStore := NewMockStoreOperate(ctrl)
	mockFileStore.EXPECT().Name().Times(2).Return("fake")
	mockFileStore.EXPECT().Init(options).Return(nil)
	err := Init("fake", options)
	assert.Error(t, err)

	Register(mockFileStore)
	err = Init("fake", options)
	assert.NoError(t, err)

	store, err := GetStoreOperate()
	assert.NoError(t, err)
	assert.NotNil(t, store)
}
