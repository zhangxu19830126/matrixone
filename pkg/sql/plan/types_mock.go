// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by MockGen. DO NOT EDIT.
// Source: ../../../pkg/sql/plan/types.go

// Package plan is a generated GoMock package.
package plan

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statsinfo "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	tree "github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	function "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	process "github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MockCompilerContext2 is a mock of CompilerContext interface.
type MockCompilerContext2 struct {
	ctrl     *gomock.Controller
	recorder *MockCompilerContext2MockRecorder
}

// MockCompilerContext2MockRecorder is the mock recorder for MockCompilerContext2.
type MockCompilerContext2MockRecorder struct {
	mock *MockCompilerContext2
}

// NewMockCompilerContext2 creates a new mock instance.
func NewMockCompilerContext2(ctrl *gomock.Controller) *MockCompilerContext2 {
	mock := &MockCompilerContext2{ctrl: ctrl}
	mock.recorder = &MockCompilerContext2MockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCompilerContext2) EXPECT() *MockCompilerContext2MockRecorder {
	return m.recorder
}

// CheckSubscriptionValid mocks base method.
func (m *MockCompilerContext2) CheckSubscriptionValid(subName, accName, pubName string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckSubscriptionValid", subName, accName, pubName)
	ret0, _ := ret[0].(error)
	return ret0
}

// CheckSubscriptionValid indicates an expected call of CheckSubscriptionValid.
func (mr *MockCompilerContext2MockRecorder) CheckSubscriptionValid(subName, accName, pubName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckSubscriptionValid", reflect.TypeOf((*MockCompilerContext2)(nil).CheckSubscriptionValid), subName, accName, pubName)
}

// DatabaseExists mocks base method.
func (m *MockCompilerContext2) DatabaseExists(name string) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DatabaseExists", name)
	ret0, _ := ret[0].(bool)
	return ret0
}

// DatabaseExists indicates an expected call of DatabaseExists.
func (mr *MockCompilerContext2MockRecorder) DatabaseExists(name interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DatabaseExists", reflect.TypeOf((*MockCompilerContext2)(nil).DatabaseExists), name)
}

// DefaultDatabase mocks base method.
func (m *MockCompilerContext2) DefaultDatabase() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DefaultDatabase")
	ret0, _ := ret[0].(string)
	return ret0
}

// DefaultDatabase indicates an expected call of DefaultDatabase.
func (mr *MockCompilerContext2MockRecorder) DefaultDatabase() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DefaultDatabase", reflect.TypeOf((*MockCompilerContext2)(nil).DefaultDatabase))
}

// GetAccountId mocks base method.
func (m *MockCompilerContext2) GetAccountId() (uint32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAccountId")
	ret0, _ := ret[0].(uint32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAccountId indicates an expected call of GetAccountId.
func (mr *MockCompilerContext2MockRecorder) GetAccountId() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAccountId", reflect.TypeOf((*MockCompilerContext2)(nil).GetAccountId))
}

// GetBuildingAlterView mocks base method.
func (m *MockCompilerContext2) GetBuildingAlterView() (bool, string, string) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBuildingAlterView")
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(string)
	return ret0, ret1, ret2
}

// GetBuildingAlterView indicates an expected call of GetBuildingAlterView.
func (mr *MockCompilerContext2MockRecorder) GetBuildingAlterView() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBuildingAlterView", reflect.TypeOf((*MockCompilerContext2)(nil).GetBuildingAlterView))
}

// GetContext mocks base method.
func (m *MockCompilerContext2) GetContext() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetContext")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// GetContext indicates an expected call of GetContext.
func (mr *MockCompilerContext2MockRecorder) GetContext() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetContext", reflect.TypeOf((*MockCompilerContext2)(nil).GetContext))
}

// GetDatabaseId mocks base method.
func (m *MockCompilerContext2) GetDatabaseId(dbName string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDatabaseId", dbName)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDatabaseId indicates an expected call of GetDatabaseId.
func (mr *MockCompilerContext2MockRecorder) GetDatabaseId(dbName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDatabaseId", reflect.TypeOf((*MockCompilerContext2)(nil).GetDatabaseId), dbName)
}

// GetPrimaryKeyDef mocks base method.
func (m *MockCompilerContext2) GetPrimaryKeyDef(dbName, tableName string) []*plan.ColDef {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPrimaryKeyDef", dbName, tableName)
	ret0, _ := ret[0].([]*plan.ColDef)
	return ret0
}

// GetPrimaryKeyDef indicates an expected call of GetPrimaryKeyDef.
func (mr *MockCompilerContext2MockRecorder) GetPrimaryKeyDef(dbName, tableName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPrimaryKeyDef", reflect.TypeOf((*MockCompilerContext2)(nil).GetPrimaryKeyDef), dbName, tableName)
}

// GetProcess mocks base method.
func (m *MockCompilerContext2) GetProcess() *process.Process {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetProcess")
	ret0, _ := ret[0].(*process.Process)
	return ret0
}

// GetProcess indicates an expected call of GetProcess.
func (mr *MockCompilerContext2MockRecorder) GetProcess() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetProcess", reflect.TypeOf((*MockCompilerContext2)(nil).GetProcess))
}

// GetQueryResultMeta mocks base method.
func (m *MockCompilerContext2) GetQueryResultMeta(uuid string) ([]*plan.ColDef, string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetQueryResultMeta", uuid)
	ret0, _ := ret[0].([]*plan.ColDef)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetQueryResultMeta indicates an expected call of GetQueryResultMeta.
func (mr *MockCompilerContext2MockRecorder) GetQueryResultMeta(uuid interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetQueryResultMeta", reflect.TypeOf((*MockCompilerContext2)(nil).GetQueryResultMeta), uuid)
}

// GetQueryingSubscription mocks base method.
func (m *MockCompilerContext2) GetQueryingSubscription() *plan.SubscriptionMeta {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetQueryingSubscription")
	ret0, _ := ret[0].(*plan.SubscriptionMeta)
	return ret0
}

// GetQueryingSubscription indicates an expected call of GetQueryingSubscription.
func (mr *MockCompilerContext2MockRecorder) GetQueryingSubscription() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetQueryingSubscription", reflect.TypeOf((*MockCompilerContext2)(nil).GetQueryingSubscription))
}

// GetRootSql mocks base method.
func (m *MockCompilerContext2) GetRootSql() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRootSql")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetRootSql indicates an expected call of GetRootSql.
func (mr *MockCompilerContext2MockRecorder) GetRootSql() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRootSql", reflect.TypeOf((*MockCompilerContext2)(nil).GetRootSql))
}

// GetStatsCache mocks base method.
func (m *MockCompilerContext2) GetStatsCache() *StatsCache {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStatsCache")
	ret0, _ := ret[0].(*StatsCache)
	return ret0
}

// GetStatsCache indicates an expected call of GetStatsCache.
func (mr *MockCompilerContext2MockRecorder) GetStatsCache() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStatsCache", reflect.TypeOf((*MockCompilerContext2)(nil).GetStatsCache))
}

// GetSubscriptionMeta mocks base method.
func (m *MockCompilerContext2) GetSubscriptionMeta(dbName string) (*plan.SubscriptionMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSubscriptionMeta", dbName)
	ret0, _ := ret[0].(*plan.SubscriptionMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSubscriptionMeta indicates an expected call of GetSubscriptionMeta.
func (mr *MockCompilerContext2MockRecorder) GetSubscriptionMeta(dbName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSubscriptionMeta", reflect.TypeOf((*MockCompilerContext2)(nil).GetSubscriptionMeta), dbName)
}

// GetUserName mocks base method.
func (m *MockCompilerContext2) GetUserName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUserName")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetUserName indicates an expected call of GetUserName.
func (mr *MockCompilerContext2MockRecorder) GetUserName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUserName", reflect.TypeOf((*MockCompilerContext2)(nil).GetUserName))
}

// IsPublishing mocks base method.
func (m *MockCompilerContext2) IsPublishing(dbName string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsPublishing", dbName)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsPublishing indicates an expected call of IsPublishing.
func (mr *MockCompilerContext2MockRecorder) IsPublishing(dbName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsPublishing", reflect.TypeOf((*MockCompilerContext2)(nil).IsPublishing), dbName)
}

// ReplacePlan mocks base method.
func (m *MockCompilerContext2) ReplacePlan(execPlan *plan.Execute) (*plan.Plan, tree.Statement, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReplacePlan", execPlan)
	ret0, _ := ret[0].(*plan.Plan)
	ret1, _ := ret[1].(tree.Statement)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ReplacePlan indicates an expected call of ReplacePlan.
func (mr *MockCompilerContext2MockRecorder) ReplacePlan(execPlan interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReplacePlan", reflect.TypeOf((*MockCompilerContext2)(nil).ReplacePlan), execPlan)
}

// Resolve mocks base method.
func (m *MockCompilerContext2) Resolve(schemaName, tableName string) (*plan.ObjectRef, *plan.TableDef) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Resolve", schemaName, tableName)
	ret0, _ := ret[0].(*plan.ObjectRef)
	ret1, _ := ret[1].(*plan.TableDef)
	return ret0, ret1
}

// Resolve indicates an expected call of Resolve.
func (mr *MockCompilerContext2MockRecorder) Resolve(schemaName, tableName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Resolve", reflect.TypeOf((*MockCompilerContext2)(nil).Resolve), schemaName, tableName)
}

// ResolveAccountIds mocks base method.
func (m *MockCompilerContext2) ResolveAccountIds(accountNames []string) ([]uint32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResolveAccountIds", accountNames)
	ret0, _ := ret[0].([]uint32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResolveAccountIds indicates an expected call of ResolveAccountIds.
func (mr *MockCompilerContext2MockRecorder) ResolveAccountIds(accountNames interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResolveAccountIds", reflect.TypeOf((*MockCompilerContext2)(nil).ResolveAccountIds), accountNames)
}

// ResolveById mocks base method.
func (m *MockCompilerContext2) ResolveById(tableId uint64) (*plan.ObjectRef, *plan.TableDef) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResolveById", tableId)
	ret0, _ := ret[0].(*plan.ObjectRef)
	ret1, _ := ret[1].(*plan.TableDef)
	return ret0, ret1
}

// ResolveById indicates an expected call of ResolveById.
func (mr *MockCompilerContext2MockRecorder) ResolveById(tableId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResolveById", reflect.TypeOf((*MockCompilerContext2)(nil).ResolveById), tableId)
}

// ResolveUdf mocks base method.
func (m *MockCompilerContext2) ResolveUdf(name string, args []*plan.Expr) (*function.Udf, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResolveUdf", name, args)
	ret0, _ := ret[0].(*function.Udf)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResolveSnapshotTsWithSnapShotName mocks base method.
func (m *MockCompilerContext2) ResolveSnapshotTsWithSnapShotName(snapshotName string) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResolveSnapshotTsWithSnapShotName", snapshotName)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResolveSnapshotTsWithSnapShotName indicates an expected call of ResolveSnapshotTsWithSnapShotName.
func (mr *MockCompilerContext2MockRecorder) ResolveSnapshotTsWithSnapShotName(snapshotName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResolveSnapshotTsWithSnapShotName", reflect.TypeOf((*MockCompilerContext)(nil).ResolveSnapshotTsWithSnapShotName), snapshotName)
}

// ResolveUdf indicates an expected call of ResolveUdf.
func (mr *MockCompilerContext2MockRecorder) ResolveUdf(name, args interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResolveUdf", reflect.TypeOf((*MockCompilerContext2)(nil).ResolveUdf), name, args)
}

// ResolveVariable mocks base method.
func (m *MockCompilerContext2) ResolveVariable(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResolveVariable", varName, isSystemVar, isGlobalVar)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ResolveVariable indicates an expected call of ResolveVariable.
func (mr *MockCompilerContext2MockRecorder) ResolveVariable(varName, isSystemVar, isGlobalVar interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResolveVariable", reflect.TypeOf((*MockCompilerContext2)(nil).ResolveVariable), varName, isSystemVar, isGlobalVar)
}

// SetBuildingAlterView mocks base method.
func (m *MockCompilerContext2) SetBuildingAlterView(yesOrNo bool, dbName, viewName string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetBuildingAlterView", yesOrNo, dbName, viewName)
}

// SetBuildingAlterView indicates an expected call of SetBuildingAlterView.
func (mr *MockCompilerContext2MockRecorder) SetBuildingAlterView(yesOrNo, dbName, viewName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetBuildingAlterView", reflect.TypeOf((*MockCompilerContext2)(nil).SetBuildingAlterView), yesOrNo, dbName, viewName)
}

// SetQueryingSubscription mocks base method.
func (m *MockCompilerContext2) SetQueryingSubscription(meta *plan.SubscriptionMeta) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetQueryingSubscription", meta)
}

// SetQueryingSubscription indicates an expected call of SetQueryingSubscription.
func (mr *MockCompilerContext2MockRecorder) SetQueryingSubscription(meta interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetQueryingSubscription", reflect.TypeOf((*MockCompilerContext2)(nil).SetQueryingSubscription), meta)
}

// Stats mocks base method.
func (m *MockCompilerContext2) Stats(obj *plan.ObjectRef) (*statsinfo.StatsInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stats", obj)
	ret0, _ := ret[0].(*statsinfo.StatsInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Stats indicates an expected call of Stats.
func (mr *MockCompilerContext2MockRecorder) Stats(obj interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stats", reflect.TypeOf((*MockCompilerContext2)(nil).Stats), obj)
}

// MockOptimizer2 is a mock of Optimizer interface.
type MockOptimizer2 struct {
	ctrl     *gomock.Controller
	recorder *MockOptimizer2MockRecorder
}

// MockOptimizer2MockRecorder is the mock recorder for MockOptimizer2.
type MockOptimizer2MockRecorder struct {
	mock *MockOptimizer2
}

// NewMockOptimizer2 creates a new mock instance.
func NewMockOptimizer2(ctrl *gomock.Controller) *MockOptimizer2 {
	mock := &MockOptimizer2{ctrl: ctrl}
	mock.recorder = &MockOptimizer2MockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockOptimizer2) EXPECT() *MockOptimizer2MockRecorder {
	return m.recorder
}

// CurrentContext mocks base method.
func (m *MockOptimizer2) CurrentContext() CompilerContext {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CurrentContext")
	ret0, _ := ret[0].(CompilerContext)
	return ret0
}

// CurrentContext indicates an expected call of CurrentContext.
func (mr *MockOptimizer2MockRecorder) CurrentContext() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CurrentContext", reflect.TypeOf((*MockOptimizer2)(nil).CurrentContext))
}

// Optimize mocks base method.
func (m *MockOptimizer2) Optimize(stmt tree.Statement) (*plan.Query, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Optimize", stmt)
	ret0, _ := ret[0].(*plan.Query)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Optimize indicates an expected call of Optimize.
func (mr *MockOptimizer2MockRecorder) Optimize(stmt interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Optimize", reflect.TypeOf((*MockOptimizer2)(nil).Optimize), stmt)
}

// MockRule is a mock of Rule interface.
type MockRule struct {
	ctrl     *gomock.Controller
	recorder *MockRuleMockRecorder
}

// MockRuleMockRecorder is the mock recorder for MockRule.
type MockRuleMockRecorder struct {
	mock *MockRule
}

// NewMockRule creates a new mock instance.
func NewMockRule(ctrl *gomock.Controller) *MockRule {
	mock := &MockRule{ctrl: ctrl}
	mock.recorder = &MockRuleMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRule) EXPECT() *MockRuleMockRecorder {
	return m.recorder
}

// Apply mocks base method.
func (m *MockRule) Apply(arg0 *plan.Node, arg1 *plan.Query, arg2 *process.Process) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Apply", arg0, arg1, arg2)
}

// Apply indicates an expected call of Apply.
func (mr *MockRuleMockRecorder) Apply(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Apply", reflect.TypeOf((*MockRule)(nil).Apply), arg0, arg1, arg2)
}

// Match mocks base method.
func (m *MockRule) Match(arg0 *plan.Node) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Match", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Match indicates an expected call of Match.
func (mr *MockRuleMockRecorder) Match(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Match", reflect.TypeOf((*MockRule)(nil).Match), arg0)
}

// MockBinder is a mock of Binder interface.
type MockBinder struct {
	ctrl     *gomock.Controller
	recorder *MockBinderMockRecorder
}

// MockBinderMockRecorder is the mock recorder for MockBinder.
type MockBinderMockRecorder struct {
	mock *MockBinder
}

// NewMockBinder creates a new mock instance.
func NewMockBinder(ctrl *gomock.Controller) *MockBinder {
	mock := &MockBinder{ctrl: ctrl}
	mock.recorder = &MockBinderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBinder) EXPECT() *MockBinderMockRecorder {
	return m.recorder
}

// BindAggFunc mocks base method.
func (m *MockBinder) BindAggFunc(arg0 string, arg1 *tree.FuncExpr, arg2 int32, arg3 bool) (*plan.Expr, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BindAggFunc", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*plan.Expr)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BindAggFunc indicates an expected call of BindAggFunc.
func (mr *MockBinderMockRecorder) BindAggFunc(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BindAggFunc", reflect.TypeOf((*MockBinder)(nil).BindAggFunc), arg0, arg1, arg2, arg3)
}

// BindColRef mocks base method.
func (m *MockBinder) BindColRef(arg0 *tree.UnresolvedName, arg1 int32, arg2 bool) (*plan.Expr, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BindColRef", arg0, arg1, arg2)
	ret0, _ := ret[0].(*plan.Expr)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BindColRef indicates an expected call of BindColRef.
func (mr *MockBinderMockRecorder) BindColRef(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BindColRef", reflect.TypeOf((*MockBinder)(nil).BindColRef), arg0, arg1, arg2)
}

// BindExpr mocks base method.
func (m *MockBinder) BindExpr(arg0 tree.Expr, arg1 int32, arg2 bool) (*plan.Expr, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BindExpr", arg0, arg1, arg2)
	ret0, _ := ret[0].(*plan.Expr)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BindExpr indicates an expected call of BindExpr.
func (mr *MockBinderMockRecorder) BindExpr(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BindExpr", reflect.TypeOf((*MockBinder)(nil).BindExpr), arg0, arg1, arg2)
}

// BindSubquery mocks base method.
func (m *MockBinder) BindSubquery(arg0 *tree.Subquery, arg1 bool) (*plan.Expr, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BindSubquery", arg0, arg1)
	ret0, _ := ret[0].(*plan.Expr)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BindSubquery indicates an expected call of BindSubquery.
func (mr *MockBinderMockRecorder) BindSubquery(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BindSubquery", reflect.TypeOf((*MockBinder)(nil).BindSubquery), arg0, arg1)
}

// BindTimeWindowFunc mocks base method.
func (m *MockBinder) BindTimeWindowFunc(arg0 string, arg1 *tree.FuncExpr, arg2 int32, arg3 bool) (*plan.Expr, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BindTimeWindowFunc", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*plan.Expr)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BindTimeWindowFunc indicates an expected call of BindTimeWindowFunc.
func (mr *MockBinderMockRecorder) BindTimeWindowFunc(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BindTimeWindowFunc", reflect.TypeOf((*MockBinder)(nil).BindTimeWindowFunc), arg0, arg1, arg2, arg3)
}

// BindWinFunc mocks base method.
func (m *MockBinder) BindWinFunc(arg0 string, arg1 *tree.FuncExpr, arg2 int32, arg3 bool) (*plan.Expr, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BindWinFunc", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*plan.Expr)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BindWinFunc indicates an expected call of BindWinFunc.
func (mr *MockBinderMockRecorder) BindWinFunc(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BindWinFunc", reflect.TypeOf((*MockBinder)(nil).BindWinFunc), arg0, arg1, arg2, arg3)
}

// GetContext mocks base method.
func (m *MockBinder) GetContext() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetContext")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// GetContext indicates an expected call of GetContext.
func (mr *MockBinderMockRecorder) GetContext() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetContext", reflect.TypeOf((*MockBinder)(nil).GetContext))
}
