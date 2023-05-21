// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/proto/runtime.go

// Package testdata is a generated GoMock package.
package testdata

import (
	context "context"
	reflect "reflect"
	time "time"
)

import (
	gomock "github.com/golang/mock/gomock"
)

import (
	proto "github.com/arana-db/arana/pkg/proto"
)

// MockVersionSupport is a mock of VersionSupport interface.
type MockVersionSupport struct {
	ctrl     *gomock.Controller
	recorder *MockVersionSupportMockRecorder
}

// MockVersionSupportMockRecorder is the mock recorder for MockVersionSupport.
type MockVersionSupportMockRecorder struct {
	mock *MockVersionSupport
}

// NewMockVersionSupport creates a new mock instance.
func NewMockVersionSupport(ctrl *gomock.Controller) *MockVersionSupport {
	mock := &MockVersionSupport{ctrl: ctrl}
	mock.recorder = &MockVersionSupportMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockVersionSupport) EXPECT() *MockVersionSupportMockRecorder {
	return m.recorder
}

// Version mocks base method.
func (m *MockVersionSupport) Version(ctx context.Context) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Version", ctx)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Version indicates an expected call of Version.
func (mr *MockVersionSupportMockRecorder) Version(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Version", reflect.TypeOf((*MockVersionSupport)(nil).Version), ctx)
}

// MockVConn is a mock of VConn interface.
type MockVConn struct {
	ctrl     *gomock.Controller
	recorder *MockVConnMockRecorder
}

// MockVConnMockRecorder is the mock recorder for MockVConn.
type MockVConnMockRecorder struct {
	mock *MockVConn
}

// NewMockVConn creates a new mock instance.
func NewMockVConn(ctrl *gomock.Controller) *MockVConn {
	mock := &MockVConn{ctrl: ctrl}
	mock.recorder = &MockVConnMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockVConn) EXPECT() *MockVConnMockRecorder {
	return m.recorder
}

// Exec mocks base method.
func (m *MockVConn) Exec(ctx context.Context, db, query string, args ...proto.Value) (proto.Result, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, db, query}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exec indicates an expected call of Exec.
func (mr *MockVConnMockRecorder) Exec(ctx, db, query interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, db, query}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockVConn)(nil).Exec), varargs...)
}

// Query mocks base method.
func (m *MockVConn) Query(ctx context.Context, db, query string, args ...proto.Value) (proto.Result, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, db, query}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Query", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Query indicates an expected call of Query.
func (mr *MockVConnMockRecorder) Query(ctx, db, query interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, db, query}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockVConn)(nil).Query), varargs...)
}

// MockPlan is a mock of Plan interface.
type MockPlan struct {
	ctrl     *gomock.Controller
	recorder *MockPlanMockRecorder
}

// MockPlanMockRecorder is the mock recorder for MockPlan.
type MockPlanMockRecorder struct {
	mock *MockPlan
}

// NewMockPlan creates a new mock instance.
func NewMockPlan(ctrl *gomock.Controller) *MockPlan {
	mock := &MockPlan{ctrl: ctrl}
	mock.recorder = &MockPlanMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPlan) EXPECT() *MockPlanMockRecorder {
	return m.recorder
}

// ExecIn mocks base method.
func (m *MockPlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecIn", ctx, conn)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecIn indicates an expected call of ExecIn.
func (mr *MockPlanMockRecorder) ExecIn(ctx, conn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecIn", reflect.TypeOf((*MockPlan)(nil).ExecIn), ctx, conn)
}

// Type mocks base method.
func (m *MockPlan) Type() proto.PlanType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Type")
	ret0, _ := ret[0].(proto.PlanType)
	return ret0
}

// Type indicates an expected call of Type.
func (mr *MockPlanMockRecorder) Type() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Type", reflect.TypeOf((*MockPlan)(nil).Type))
}

// MockOptimizer is a mock of Optimizer interface.
type MockOptimizer struct {
	ctrl     *gomock.Controller
	recorder *MockOptimizerMockRecorder
}

// MockOptimizerMockRecorder is the mock recorder for MockOptimizer.
type MockOptimizerMockRecorder struct {
	mock *MockOptimizer
}

// NewMockOptimizer creates a new mock instance.
func NewMockOptimizer(ctrl *gomock.Controller) *MockOptimizer {
	mock := &MockOptimizer{ctrl: ctrl}
	mock.recorder = &MockOptimizerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockOptimizer) EXPECT() *MockOptimizerMockRecorder {
	return m.recorder
}

// Optimize mocks base method.
func (m *MockOptimizer) Optimize(ctx context.Context) (proto.Plan, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Optimize", ctx)
	ret0, _ := ret[0].(proto.Plan)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Optimize indicates an expected call of Optimize.
func (mr *MockOptimizerMockRecorder) Optimize(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Optimize", reflect.TypeOf((*MockOptimizer)(nil).Optimize), ctx)
}

// MockCallable is a mock of Callable interface.
type MockCallable struct {
	ctrl     *gomock.Controller
	recorder *MockCallableMockRecorder
}

// MockCallableMockRecorder is the mock recorder for MockCallable.
type MockCallableMockRecorder struct {
	mock *MockCallable
}

// NewMockCallable creates a new mock instance.
func NewMockCallable(ctrl *gomock.Controller) *MockCallable {
	mock := &MockCallable{ctrl: ctrl}
	mock.recorder = &MockCallableMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCallable) EXPECT() *MockCallableMockRecorder {
	return m.recorder
}

// Call mocks base method.
func (m *MockCallable) Call(ctx context.Context, sql string, args ...proto.Value) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, sql}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Call", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Call indicates an expected call of Call.
func (mr *MockCallableMockRecorder) Call(ctx, sql interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, sql}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Call", reflect.TypeOf((*MockCallable)(nil).Call), varargs...)
}

// CallFieldList mocks base method.
func (m *MockCallable) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CallFieldList", ctx, table, wildcard)
	ret0, _ := ret[0].([]proto.Field)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CallFieldList indicates an expected call of CallFieldList.
func (mr *MockCallableMockRecorder) CallFieldList(ctx, table, wildcard interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CallFieldList", reflect.TypeOf((*MockCallable)(nil).CallFieldList), ctx, table, wildcard)
}

// MockDB is a mock of DB interface.
type MockDB struct {
	ctrl     *gomock.Controller
	recorder *MockDBMockRecorder
}

// MockDBMockRecorder is the mock recorder for MockDB.
type MockDBMockRecorder struct {
	mock *MockDB
}

// NewMockDB creates a new mock instance.
func NewMockDB(ctrl *gomock.Controller) *MockDB {
	mock := &MockDB{ctrl: ctrl}
	mock.recorder = &MockDBMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDB) EXPECT() *MockDBMockRecorder {
	return m.recorder
}

// Call mocks base method.
func (m *MockDB) Call(ctx context.Context, sql string, args ...proto.Value) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, sql}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Call", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Call indicates an expected call of Call.
func (mr *MockDBMockRecorder) Call(ctx, sql interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, sql}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Call", reflect.TypeOf((*MockDB)(nil).Call), varargs...)
}

// CallFieldList mocks base method.
func (m *MockDB) CallFieldList(ctx context.Context, table, wildcard string) ([]proto.Field, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CallFieldList", ctx, table, wildcard)
	ret0, _ := ret[0].([]proto.Field)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CallFieldList indicates an expected call of CallFieldList.
func (mr *MockDBMockRecorder) CallFieldList(ctx, table, wildcard interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CallFieldList", reflect.TypeOf((*MockDB)(nil).CallFieldList), ctx, table, wildcard)
}

// Capacity mocks base method.
func (m *MockDB) Capacity() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Capacity")
	ret0, _ := ret[0].(int)
	return ret0
}

// Capacity indicates an expected call of Capacity.
func (mr *MockDBMockRecorder) Capacity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Capacity", reflect.TypeOf((*MockDB)(nil).Capacity))
}

// Close mocks base method.
func (m *MockDB) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockDBMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockDB)(nil).Close))
}

// ID mocks base method.
func (m *MockDB) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockDBMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockDB)(nil).ID))
}

// IdleTimeout mocks base method.
func (m *MockDB) IdleTimeout() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IdleTimeout")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// IdleTimeout indicates an expected call of IdleTimeout.
func (mr *MockDBMockRecorder) IdleTimeout() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IdleTimeout", reflect.TypeOf((*MockDB)(nil).IdleTimeout))
}

// MaxCapacity mocks base method.
func (m *MockDB) MaxCapacity() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MaxCapacity")
	ret0, _ := ret[0].(int)
	return ret0
}

// MaxCapacity indicates an expected call of MaxCapacity.
func (mr *MockDBMockRecorder) MaxCapacity() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MaxCapacity", reflect.TypeOf((*MockDB)(nil).MaxCapacity))
}

// NodeConn mocks base method.
func (m *MockDB) NodeConn() proto.NodeConn {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NodeConn")
	ret0, _ := ret[0].(proto.NodeConn)
	return ret0
}

// NodeConn indicates an expected call of NodeConn.
func (mr *MockDBMockRecorder) NodeConn() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NodeConn", reflect.TypeOf((*MockDB)(nil).NodeConn))
}

// SetCapacity mocks base method.
func (m *MockDB) SetCapacity(capacity int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetCapacity", capacity)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetCapacity indicates an expected call of SetCapacity.
func (mr *MockDBMockRecorder) SetCapacity(capacity interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetCapacity", reflect.TypeOf((*MockDB)(nil).SetCapacity), capacity)
}

// SetIdleTimeout mocks base method.
func (m *MockDB) SetIdleTimeout(idleTimeout time.Duration) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetIdleTimeout", idleTimeout)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetIdleTimeout indicates an expected call of SetIdleTimeout.
func (mr *MockDBMockRecorder) SetIdleTimeout(idleTimeout interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetIdleTimeout", reflect.TypeOf((*MockDB)(nil).SetIdleTimeout), idleTimeout)
}

// SetMaxCapacity mocks base method.
func (m *MockDB) SetMaxCapacity(maxCapacity int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetMaxCapacity", maxCapacity)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetMaxCapacity indicates an expected call of SetMaxCapacity.
func (mr *MockDBMockRecorder) SetMaxCapacity(maxCapacity interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetMaxCapacity", reflect.TypeOf((*MockDB)(nil).SetMaxCapacity), maxCapacity)
}

// SetWeight mocks base method.
func (m *MockDB) SetWeight(weight proto.Weight) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetWeight", weight)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetWeight indicates an expected call of SetWeight.
func (mr *MockDBMockRecorder) SetWeight(weight interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetWeight", reflect.TypeOf((*MockDB)(nil).SetWeight), weight)
}

// Variable mocks base method.
func (m *MockDB) Variable(ctx context.Context, name string) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Variable", ctx, name)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Variable indicates an expected call of Variable.
func (mr *MockDBMockRecorder) Variable(ctx, name interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Variable", reflect.TypeOf((*MockDB)(nil).Variable), ctx, name)
}

// Weight mocks base method.
func (m *MockDB) Weight() proto.Weight {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Weight")
	ret0, _ := ret[0].(proto.Weight)
	return ret0
}

// Weight indicates an expected call of Weight.
func (mr *MockDBMockRecorder) Weight() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Weight", reflect.TypeOf((*MockDB)(nil).Weight))
}

// MockExecutable is a mock of Executable interface.
type MockExecutable struct {
	ctrl     *gomock.Controller
	recorder *MockExecutableMockRecorder
}

// MockExecutableMockRecorder is the mock recorder for MockExecutable.
type MockExecutableMockRecorder struct {
	mock *MockExecutable
}

// NewMockExecutable creates a new mock instance.
func NewMockExecutable(ctrl *gomock.Controller) *MockExecutable {
	mock := &MockExecutable{ctrl: ctrl}
	mock.recorder = &MockExecutableMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockExecutable) EXPECT() *MockExecutableMockRecorder {
	return m.recorder
}

// Execute mocks base method.
func (m *MockExecutable) Execute(ctx *proto.Context) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", ctx)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Execute indicates an expected call of Execute.
func (mr *MockExecutableMockRecorder) Execute(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockExecutable)(nil).Execute), ctx)
}

// MockTx is a mock of Tx interface.
type MockTx struct {
	ctrl     *gomock.Controller
	recorder *MockTxMockRecorder
}

// MockTxMockRecorder is the mock recorder for MockTx.
type MockTxMockRecorder struct {
	mock *MockTx
}

// NewMockTx creates a new mock instance.
func NewMockTx(ctrl *gomock.Controller) *MockTx {
	mock := &MockTx{ctrl: ctrl}
	mock.recorder = &MockTxMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTx) EXPECT() *MockTxMockRecorder {
	return m.recorder
}

// Commit mocks base method.
func (m *MockTx) Commit(ctx context.Context) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", ctx)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Commit indicates an expected call of Commit.
func (mr *MockTxMockRecorder) Commit(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockTx)(nil).Commit), ctx)
}

// Exec mocks base method.
func (m *MockTx) Exec(ctx context.Context, db, query string, args ...proto.Value) (proto.Result, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, db, query}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exec indicates an expected call of Exec.
func (mr *MockTxMockRecorder) Exec(ctx, db, query interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, db, query}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockTx)(nil).Exec), varargs...)
}

// Execute mocks base method.
func (m *MockTx) Execute(ctx *proto.Context) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", ctx)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Execute indicates an expected call of Execute.
func (mr *MockTxMockRecorder) Execute(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockTx)(nil).Execute), ctx)
}

// ID mocks base method.
func (m *MockTx) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockTxMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockTx)(nil).ID))
}

// Query mocks base method.
func (m *MockTx) Query(ctx context.Context, db, query string, args ...proto.Value) (proto.Result, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, db, query}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Query", varargs...)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Query indicates an expected call of Query.
func (mr *MockTxMockRecorder) Query(ctx, db, query interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, db, query}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockTx)(nil).Query), varargs...)
}

// Rollback mocks base method.
func (m *MockTx) Rollback(ctx context.Context) (proto.Result, uint16, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback", ctx)
	ret0, _ := ret[0].(proto.Result)
	ret1, _ := ret[1].(uint16)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Rollback indicates an expected call of Rollback.
func (mr *MockTxMockRecorder) Rollback(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockTx)(nil).Rollback), ctx)
}
