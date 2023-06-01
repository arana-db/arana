package dataset

import "github.com/arana-db/arana/pkg/runtime/ast"

type CompareI interface {
	Compare(outerValue, innerValue string)
}

type InnerJoinCompare struct {
}

func (i *InnerJoinCompare) Compare(outerValue, innerValue string) {
	if outerValue == innerValue {
		return
	}
}

type LeftJoinCompare struct {
}

func (l *LeftJoinCompare) Compare(outerValue, innerValue string) {

}

type RightJoinCompare struct {
}

func (r *RightJoinCompare) Compare(outerValue, innerValue string) {

}

func CompareInstance(joinType ast.JoinType) CompareI {
	switch joinType {
	case ast.InnerJoin:
		return &InnerJoinCompare{}
	case ast.LeftJoin:
		return &LeftJoinCompare{}
	case ast.RightJoin:
		return &RightJoinCompare{}
	}
	return nil
}
