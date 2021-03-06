// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/arana-db/arana/pkg/proto (interfaces: SchemaLoader)

// Package testdata is a generated GoMock package.
package testdata

import (
	context "context"
	reflect "reflect"
)

import (
	gomock "github.com/golang/mock/gomock"
)

import (
	proto "github.com/arana-db/arana/pkg/proto"
)

// MockSchemaLoader is a mock of SchemaLoader interface.
type MockSchemaLoader struct {
	ctrl     *gomock.Controller
	recorder *MockSchemaLoaderMockRecorder
}

// MockSchemaLoaderMockRecorder is the mock recorder for MockSchemaLoader.
type MockSchemaLoaderMockRecorder struct {
	mock *MockSchemaLoader
}

// NewMockSchemaLoader creates a new mock instance.
func NewMockSchemaLoader(ctrl *gomock.Controller) *MockSchemaLoader {
	mock := &MockSchemaLoader{ctrl: ctrl}
	mock.recorder = &MockSchemaLoaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSchemaLoader) EXPECT() *MockSchemaLoaderMockRecorder {
	return m.recorder
}

// Load mocks base method.
func (m *MockSchemaLoader) Load(arg0 context.Context, arg1 string, arg2 []string) (map[string]*proto.TableMetadata, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Load", arg0, arg1, arg2)
	ret0, _ := ret[0].(map[string]*proto.TableMetadata)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Load indicates an expected call of Load.
func (mr *MockSchemaLoaderMockRecorder) Load(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Load", reflect.TypeOf((*MockSchemaLoader)(nil).Load), arg0, arg1, arg2)
}
