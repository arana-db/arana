package mysql

import (
	"github.com/dubbogo/kylin/pkg/proto"
)

type Result struct {
	Fields       []proto.Field // Columns information
	AffectedRows uint64
	InsertId     uint64
	Rows         []proto.Row
}

func (res *Result) LastInsertId() (uint64, error) {
	return res.InsertId, nil
}

func (res *Result) RowsAffected() (uint64, error) {
	return res.AffectedRows, nil
}
