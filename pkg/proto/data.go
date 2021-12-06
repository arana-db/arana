package proto

import (
	"github.com/pingcap/parser/ast"
)

import (
	"github.com/dubbogo/kylin/pkg/constants/mysql"
)

type (
	Value struct {
		Typ   mysql.FieldType
		Flags uint
		Len   int
		Val   interface{}
		Raw   []byte
	}

	Field interface {
		TableName() string

		DataBaseName() string

		TypeDatabaseName() string
	}

	// Rows is an iterator over an executed query's results.
	Row interface {
		// Columns returns the names of the columns. The number of
		// columns of the result is inferred from the length of the
		// slice. If a particular column name isn't known, an empty
		// string should be returned for that entry.
		Columns() []string

		Fields() []Field

		// Data
		Data() []byte

		Decode() ([]*Value, error)
	}

	// Result is the result of a query execution.
	Result interface {
		// LastInsertId returns the database's auto-generated ID
		// after, for example, an INSERT into a table with primary
		// key.
		LastInsertId() (uint64, error)

		// RowsAffected returns the number of rows affected by the
		// query.
		RowsAffected() (uint64, error)
	}

	// Stmt is a buffer used for store prepare statement meta data
	Stmt struct {
		StatementID uint32
		PrepareStmt string
		ParamsCount uint16
		ParamsType  []int32
		ColumnNames []string
		BindVars    map[string]interface{}
		StmtNode    ast.StmtNode
	}
)
