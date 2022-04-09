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

package testdata

import (
	"reflect"
)

import (
	"github.com/golang/mock/gomock"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// MockRow is a mock of Row interface.
type MockRow struct {
	ctrl     *gomock.Controller
	recorder *MockRowMockRecorder
}

// MockRowMockRecorder is the mock recorder for MockRow.
type MockRowMockRecorder struct {
	mock *MockRow
}

// NewMockRow creates a new mock instance.
func NewMockRow(ctrl *gomock.Controller) *MockRow {
	mock := &MockRow{ctrl: ctrl}
	mock.recorder = &MockRowMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRow) EXPECT() *MockRowMockRecorder {
	return m.recorder
}

// Columns mocks base method.
func (m *MockRow) Columns() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Columns")
	ret0, _ := ret[0].([]string)
	return ret0
}

// Columns indicates an expected call of Columns.
func (mr *MockRowMockRecorder) Columns() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Columns", reflect.TypeOf((*MockRow)(nil).Columns))
}

// Data mocks base method.
func (m *MockRow) Data() []byte {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Data")
	ret0, _ := ret[0].([]byte)
	return ret0
}

// Data indicates an expected call of Data.
func (mr *MockRowMockRecorder) Data() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Data", reflect.TypeOf((*MockRow)(nil).Data))
}

// Encode mocks base method.
func (m *MockRow) Encode(values []*proto.Value, columns []proto.Field, columnNames []string) proto.Row {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Encode")
	ret0, _ := ret[0].(proto.Row)
	return ret0
}

// Encode mocks base method.
func (mr *MockRowMockRecorder) Encode() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Encode", reflect.TypeOf((*MockRow)(nil).Encode))
}

// Decode mocks base method.
func (m *MockRow) Decode() ([]*proto.Value, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Decode")
	ret0, _ := ret[0].([]*proto.Value)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Decode indicates an expected call of Decode.
func (mr *MockRowMockRecorder) Decode() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Decode", reflect.TypeOf((*MockRow)(nil).Decode))
}

// Fields mocks base method.
func (m *MockRow) Fields() []proto.Field {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Fields")
	ret0, _ := ret[0].([]proto.Field)
	return ret0
}

// Fields indicates an expected call of Fields.
func (mr *MockRowMockRecorder) Fields() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Fields", reflect.TypeOf((*MockRow)(nil).Fields))
}

// GetColumnValue mocks base method.
func (m *MockRow) GetColumnValue(column string) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetColumnValue", column)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetColumnValue indicates an expected call of GetColumnValue.
func (mr *MockRowMockRecorder) GetColumnValue(column interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetColumnValue", reflect.TypeOf((*MockRow)(nil).GetColumnValue), column)
}
