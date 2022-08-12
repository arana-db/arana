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

package ast

// Node represents a ast node.
type Node interface {
	// Accept accepts a visitor.
	Accept(visitor Visitor) (interface{}, error)
}

// Visitor represents a visitor to visit AST nodes recursively.
type Visitor interface {
	// VisitSelectStatement visits select statement.
	VisitSelectStatement(node *SelectStatement) (interface{}, error)

	// VisitSelectElementWildcard visits select element of wildcard.
	VisitSelectElementWildcard(node *SelectElementAll) (interface{}, error)

	VisitSelectElementColumn(node *SelectElementColumn) (interface{}, error)
	VisitSelectElementFunction(node *SelectElementFunction) (interface{}, error)
	VisitSelectElementExpr(node *SelectElementExpr) (interface{}, error)
	VisitTableSource(node *TableSourceNode) (interface{}, error)
	VisitTableName(node TableName) (interface{}, error)
	VisitLimit(node *LimitNode) (interface{}, error)
	VisitGroupBy(node *GroupByNode) (interface{}, error)
	VisitGroupByItem(node *GroupByItem) (interface{}, error)
	VisitOrderBy(node OrderByNode) (interface{}, error)
	VisitOrderByItem(node *OrderByItem) (interface{}, error)
	VisitLogicalExpression(node *LogicalExpressionNode) (interface{}, error)
	VisitNotExpression(node *NotExpressionNode) (interface{}, error)
	VisitPredicateExpression(node *PredicateExpressionNode) (interface{}, error)
	VisitPredicateAtom(node *AtomPredicateNode) (interface{}, error)
	VisitPredicateBetween(node *BetweenPredicateNode) (interface{}, error)
	VisitPredicateBinaryComparison(node *BinaryComparisonPredicateNode) (interface{}, error)
	VisitPredicateIn(node *InPredicateNode) (interface{}, error)
	VisitPredicateLike(node *LikePredicateNode) (interface{}, error)
	VisitPredicateRegexp(node *RegexpPredicationNode) (interface{}, error)
	VisitAtomColumn(node ColumnNameExpressionAtom) (interface{}, error)
	VisitAtomConstant(node *ConstantExpressionAtom) (interface{}, error)
	VisitAtomFunction(node *FunctionCallExpressionAtom) (interface{}, error)
	VisitAtomNested(node *NestedExpressionAtom) (interface{}, error)
	VisitAtomUnary(node *UnaryExpressionAtom) (interface{}, error)
	VisitAtomMath(node *MathExpressionAtom) (interface{}, error)
	VisitAtomSystemVariable(node *SystemVariableExpressionAtom) (interface{}, error)
	VisitAtomVariable(node VariableExpressionAtom) (interface{}, error)
	VisitAtomInterval(node *IntervalExpressionAtom) (interface{}, error)
	VisitFunction(node *Function) (interface{}, error)
	VisitFunctionAggregate(node *AggrFunction) (interface{}, error)
	VisitFunctionCast(node *CastFunction) (interface{}, error)
	VisitFunctionCaseWhenElse(node *CaseWhenElseFunction) (interface{}, error)
	VisitFunctionArg(node *FunctionArg) (interface{}, error)
}

var (
	_ Visitor = (*BaseVisitor)(nil)
	_ Visitor = (*AlwaysReturnSelfVisitor)(nil)
)

type BaseVisitor struct{}

func (b BaseVisitor) VisitSelectStatement(node *SelectStatement) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitSelectElementWildcard(node *SelectElementAll) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitSelectElementColumn(node *SelectElementColumn) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitSelectElementFunction(node *SelectElementFunction) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitSelectElementExpr(node *SelectElementExpr) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitTableSource(node *TableSourceNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitTableName(node TableName) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitLimit(node *LimitNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitGroupBy(node *GroupByNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitGroupByItem(node *GroupByItem) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitOrderBy(node OrderByNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitOrderByItem(node *OrderByItem) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitLogicalExpression(node *LogicalExpressionNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitNotExpression(node *NotExpressionNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateExpression(node *PredicateExpressionNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateAtom(node *AtomPredicateNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateBetween(node *BetweenPredicateNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateBinaryComparison(node *BinaryComparisonPredicateNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateIn(node *InPredicateNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateLike(node *LikePredicateNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitPredicateRegexp(node *RegexpPredicationNode) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomColumn(node ColumnNameExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomConstant(node *ConstantExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomFunction(node *FunctionCallExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomNested(node *NestedExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomUnary(node *UnaryExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomMath(node *MathExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomSystemVariable(node *SystemVariableExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomVariable(node VariableExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitAtomInterval(node *IntervalExpressionAtom) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitFunction(node *Function) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitFunctionAggregate(node *AggrFunction) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitFunctionCast(node *CastFunction) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitFunctionCaseWhenElse(node *CaseWhenElseFunction) (interface{}, error) {
	panic("implement me")
}

func (b BaseVisitor) VisitFunctionArg(node *FunctionArg) (interface{}, error) {
	panic("implement me")
}

type AlwaysReturnSelfVisitor struct{}

func (a AlwaysReturnSelfVisitor) VisitSelectStatement(node *SelectStatement) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitSelectElementWildcard(node *SelectElementAll) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitSelectElementColumn(node *SelectElementColumn) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitSelectElementFunction(node *SelectElementFunction) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitSelectElementExpr(node *SelectElementExpr) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitTableSource(node *TableSourceNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitTableName(node TableName) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitLimit(node *LimitNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitGroupBy(node *GroupByNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitGroupByItem(node *GroupByItem) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitOrderBy(node OrderByNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitOrderByItem(node *OrderByItem) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitLogicalExpression(node *LogicalExpressionNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitNotExpression(node *NotExpressionNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateExpression(node *PredicateExpressionNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateAtom(node *AtomPredicateNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateBetween(node *BetweenPredicateNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateBinaryComparison(node *BinaryComparisonPredicateNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateIn(node *InPredicateNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateLike(node *LikePredicateNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitPredicateRegexp(node *RegexpPredicationNode) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomColumn(node ColumnNameExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomConstant(node *ConstantExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomFunction(node *FunctionCallExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomNested(node *NestedExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomUnary(node *UnaryExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomMath(node *MathExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomSystemVariable(node *SystemVariableExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomVariable(node VariableExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitAtomInterval(node *IntervalExpressionAtom) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitFunction(node *Function) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitFunctionAggregate(node *AggrFunction) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitFunctionCast(node *CastFunction) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitFunctionCaseWhenElse(node *CaseWhenElseFunction) (interface{}, error) {
	return node, nil
}

func (a AlwaysReturnSelfVisitor) VisitFunctionArg(node *FunctionArg) (interface{}, error) {
	return node, nil
}
