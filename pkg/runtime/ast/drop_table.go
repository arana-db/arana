package ast

import (
	"github.com/pingcap/errors"
	"strings"
)

type DropTableStatement struct {
	Tables []*TableName
}

func NewDropTableStatement() *DropTableStatement {
	return &DropTableStatement{
		Tables: []*TableName{},
	}
}

func (d DropTableStatement) Restore(flag RestoreFlag, sb *strings.Builder, args *[]int) error {
	sb.WriteString("DROP TABLE ")
	for index, table := range d.Tables {
		if index != 0 {
			sb.WriteString(", ")
		}
		if err := table.Restore(flag, sb, args); err != nil {
			return errors.Annotatef(err, "An error occurred while restore DropTableStatement.Tables[%d]", index)
		}
	}
	return nil
}

func (d DropTableStatement) CntParams() int {
	return 0
}

func (d DropTableStatement) Validate() error {
	return nil
}

func (d DropTableStatement) Mode() SQLType {
	return SdropTable
}
