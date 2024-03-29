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

// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/arana-db/arana/pkg/proto (interfaces: FrontConn)

// Package testdata is a generated GoMock package.
package testdata

import (
	reflect "reflect"
)

import (
	gomock "github.com/golang/mock/gomock"
)

import (
	proto "github.com/arana-db/arana/pkg/proto"
)

// MockFrontConn is a mock of FrontConn interface.
type MockFrontConn struct {
	ctrl     *gomock.Controller
	recorder *MockFrontConnMockRecorder
}

// MockFrontConnMockRecorder is the mock recorder for MockFrontConn.
type MockFrontConnMockRecorder struct {
	mock *MockFrontConn
}

// NewMockFrontConn creates a new mock instance.
func NewMockFrontConn(ctrl *gomock.Controller) *MockFrontConn {
	mock := &MockFrontConn{ctrl: ctrl}
	mock.recorder = &MockFrontConnMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFrontConn) EXPECT() *MockFrontConnMockRecorder {
	return m.recorder
}

// CharacterSet mocks base method.
func (m *MockFrontConn) CharacterSet() byte {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CharacterSet")
	ret0, _ := ret[0].(byte)
	return ret0
}

// CharacterSet indicates an expected call of CharacterSet.
func (mr *MockFrontConnMockRecorder) CharacterSet() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CharacterSet", reflect.TypeOf((*MockFrontConn)(nil).CharacterSet))
}

// ID mocks base method.
func (m *MockFrontConn) ID() uint32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(uint32)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockFrontConnMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockFrontConn)(nil).ID))
}

// Schema mocks base method.
func (m *MockFrontConn) Schema() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Schema")
	ret0, _ := ret[0].(string)
	return ret0
}

// Schema indicates an expected call of Schema.
func (mr *MockFrontConnMockRecorder) Schema() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Schema", reflect.TypeOf((*MockFrontConn)(nil).Schema))
}

// ServerVersion mocks base method.
func (m *MockFrontConn) ServerVersion() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ServerVersion")
	ret0, _ := ret[0].(string)
	return ret0
}

// ServerVersion indicates an expected call of ServerVersion.
func (mr *MockFrontConnMockRecorder) ServerVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ServerVersion", reflect.TypeOf((*MockFrontConn)(nil).ServerVersion))
}

// SetSchema mocks base method.
func (m *MockFrontConn) SetSchema(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetSchema", arg0)
}

// SetSchema indicates an expected call of SetSchema.
func (mr *MockFrontConnMockRecorder) SetSchema(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetSchema", reflect.TypeOf((*MockFrontConn)(nil).SetSchema), arg0)
}

// SetTenant mocks base method.
func (m *MockFrontConn) SetTenant(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTenant", arg0)
}

// SetTenant indicates an expected call of SetTenant.
func (mr *MockFrontConnMockRecorder) SetTenant(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTenant", reflect.TypeOf((*MockFrontConn)(nil).SetTenant), arg0)
}

// SetTransientVariables mocks base method.
func (m *MockFrontConn) SetTransientVariables(arg0 map[string]proto.Value) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTransientVariables", arg0)
}

// SetTransientVariables indicates an expected call of SetTransientVariables.
func (mr *MockFrontConnMockRecorder) SetTransientVariables(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTransientVariables", reflect.TypeOf((*MockFrontConn)(nil).SetTransientVariables), arg0)
}

// Tenant mocks base method.
func (m *MockFrontConn) Tenant() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tenant")
	ret0, _ := ret[0].(string)
	return ret0
}

// Tenant indicates an expected call of Tenant.
func (mr *MockFrontConnMockRecorder) Tenant() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tenant", reflect.TypeOf((*MockFrontConn)(nil).Tenant))
}

// TransientVariables mocks base method.
func (m *MockFrontConn) TransientVariables() map[string]proto.Value {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TransientVariables")
	ret0, _ := ret[0].(map[string]proto.Value)
	return ret0
}

// TransientVariables indicates an expected call of TransientVariables.
func (mr *MockFrontConnMockRecorder) TransientVariables() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TransientVariables", reflect.TypeOf((*MockFrontConn)(nil).TransientVariables))
}
